#!/usr/bin/env python3
"""Register a SurveyCTO form ID for ingestion.

Usage:
    python scripts/register_surveycto_form.py my_form_id
    python scripts/register_surveycto_form.py my_form_id --schema client_acme
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from etl.surveycto_registry import get_form_config, normalize_form_config, register_form
from etl.utils import build_db_url, safe_id


load_dotenv(os.path.join(PROJECT_ROOT, "secrets", ".env"))


def probe_surveycto(form_id: str) -> tuple[bool, str]:
    """Return whether the configured SurveyCTO account can download the form."""
    import requests

    server = os.getenv("SURVEYCTO_SERVER_URL", "").rstrip("/")
    user = os.getenv("SURVEYCTO_USERNAME", "")
    password = os.getenv("SURVEYCTO_PASSWORD", "")
    if not server or not user or not password:
        return False, "SURVEYCTO_SERVER_URL, SURVEYCTO_USERNAME, or SURVEYCTO_PASSWORD is missing"

    url = f"{server}/api/v2/forms/data/wide/json/{form_id}"
    future_ms = int(datetime(2099, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    response = requests.get(url, params={"date": future_ms}, auth=(user, password), timeout=30)
    if response.status_code == 200:
        return True, f"SurveyCTO data endpoint is accessible: {url}"
    if response.status_code == 401:
        return False, "SurveyCTO rejected the username/password in secrets/.env"
    if response.status_code == 403:
        return False, (
            "SurveyCTO returned 403. Confirm the form ID exists and this user has "
            "'API access' plus 'Can download data' for that form."
        )
    if response.status_code == 404:
        return False, "SurveyCTO returned 404. Check the form ID spelling and server URL."
    return False, f"SurveyCTO returned HTTP {response.status_code}: {response.text[:250]}"


def ensure_schema(schema: str) -> None:
    """Create the warehouse schema and grants needed by ETL/analytics users."""
    safe_schema = safe_id(schema)
    engine = create_engine(build_db_url())
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{safe_schema}"'))
        conn.execute(text(f'GRANT USAGE ON SCHEMA "{safe_schema}" TO etl_writer'))
        conn.execute(text(f'GRANT USAGE ON SCHEMA "{safe_schema}" TO analyst_reader'))
        conn.execute(text(f'GRANT USAGE ON SCHEMA "{safe_schema}" TO metabase_app'))
        conn.execute(text(f'GRANT INSERT, UPDATE, SELECT, DELETE ON ALL TABLES IN SCHEMA "{safe_schema}" TO etl_writer'))
        conn.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{safe_schema}" TO analyst_reader'))
        conn.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{safe_schema}" TO metabase_app'))
        conn.execute(text(f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{safe_schema}" GRANT INSERT, UPDATE, SELECT, DELETE ON TABLES TO etl_writer'))
        conn.execute(text(f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{safe_schema}" GRANT SELECT ON TABLES TO analyst_reader'))
        conn.execute(text(f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{safe_schema}" GRANT SELECT ON TABLES TO metabase_app'))


def upsert_registered_form(form_id: str, config: dict) -> None:
    """Persist registry metadata inside qc_system for dashboards."""
    engine = create_engine(build_db_url())
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS qc_system.registered_forms (
                form_id        TEXT PRIMARY KEY,
                client         TEXT NOT NULL,
                client_schema  TEXT NOT NULL,
                table_name     TEXT NOT NULL,
                active         BOOLEAN NOT NULL DEFAULT TRUE,
                etl_deployment TEXT,
                qc_deployment  TEXT,
                registered_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE OR REPLACE FUNCTION qc_system.table_row_count(
                schema_name TEXT,
                table_name TEXT
            ) RETURNS BIGINT
            LANGUAGE plpgsql
            AS $$
            DECLARE
                result BIGINT;
            BEGIN
                IF to_regclass(format('%I.%I', schema_name, table_name)) IS NULL THEN
                    RETURN 0;
                END IF;

                EXECUTE format('SELECT count(*) FROM %I.%I', schema_name, table_name)
                INTO result;

                RETURN COALESCE(result, 0);
            END
            $$;
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS qc_system.pipeline_sla (
                pipeline_name             TEXT PRIMARY KEY,
                expected_interval_minutes INTEGER NOT NULL,
                max_lag_minutes           INTEGER NOT NULL,
                owner                     TEXT,
                created_at                TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            INSERT INTO qc_system.registered_forms (
                form_id,
                client,
                client_schema,
                table_name,
                active,
                etl_deployment,
                qc_deployment,
                updated_at
            )
            VALUES (
                :form_id,
                :client,
                :client_schema,
                :table_name,
                :active,
                :etl_deployment,
                :qc_deployment,
                NOW()
            )
            ON CONFLICT (form_id) DO UPDATE SET
                client         = EXCLUDED.client,
                client_schema  = EXCLUDED.client_schema,
                table_name     = EXCLUDED.table_name,
                active         = EXCLUDED.active,
                etl_deployment = EXCLUDED.etl_deployment,
                qc_deployment  = EXCLUDED.qc_deployment,
                updated_at     = NOW()
        """), {
            "form_id": form_id,
            "client": config["client"],
            "client_schema": config["schema"],
            "table_name": safe_id(form_id),
            "active": config["active"],
            "etl_deployment": config["etl_deployment"],
            "qc_deployment": config["qc_deployment"],
        })
        if config["active"]:
            conn.execute(text("""
                INSERT INTO qc_system.pipeline_sla (
                    pipeline_name,
                    expected_interval_minutes,
                    max_lag_minutes,
                    owner
                )
                VALUES (:form_id, 1440, 1800, 'data_team')
                ON CONFLICT (pipeline_name) DO UPDATE SET
                    expected_interval_minutes = EXCLUDED.expected_interval_minutes,
                    max_lag_minutes           = EXCLUDED.max_lag_minutes,
                    owner                     = EXCLUDED.owner
            """), {"form_id": form_id})
        else:
            conn.execute(
                text("DELETE FROM qc_system.pipeline_sla WHERE pipeline_name = :form_id"),
                {"form_id": form_id},
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Register a SurveyCTO form ID for ingestion")
    parser.add_argument("form_id", help="Exact SurveyCTO form ID")
    parser.add_argument("--client", help="Optional logical client name. Defaults to the schema name.")
    parser.add_argument("--schema", help="Optional warehouse schema. Defaults to client_<form_id>.")
    parser.add_argument("--skip-probe", action="store_true", help="Register without checking SurveyCTO access first")
    parser.add_argument("--inactive", action="store_true", help="Add the form but do not schedule/test it yet")
    args = parser.parse_args()

    if not args.skip_probe:
        ok, detail = probe_surveycto(args.form_id)
        if not ok:
            print("SurveyCTO probe failed:")
            print(f"  {detail}")
            print("\nNothing was registered. Fix SurveyCTO access or use --skip-probe intentionally.")
            return 1
        print(detail)

    existing = get_form_config(args.form_id, active_only=False) or {}
    candidate = normalize_form_config(
        args.form_id,
        {
            **existing,
            "active": not args.inactive,
            "client": args.client or existing.get("client") or args.schema,
            "schema": args.schema or existing.get("schema") or args.client,
        },
    )
    ensure_schema(candidate["schema"])
    config = register_form(
        args.form_id,
        client=candidate["client"],
        schema=candidate["schema"],
        active=not args.inactive,
    )
    upsert_registered_form(args.form_id, config)

    print("\nRegistered SurveyCTO form:")
    print(f"  form_id        : {args.form_id}")
    print(f"  client         : {config['client']}")
    print(f"  schema         : {config['schema']}")
    print(f"  etl deployment : {config['etl_deployment']}")
    print(f"  qc deployment  : {config['qc_deployment']}")
    print("\nNext checks:")
    print(f"  python scripts\\test_surveycto.py --form {args.form_id} --fetch-only")
    print(f"  python etl\\pipelines\\sync_surveycto.py {args.form_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
