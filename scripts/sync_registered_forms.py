#!/usr/bin/env python3
"""Sync config/surveycto_forms.json into qc_system.registered_forms."""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

import psycopg2


ROOT = Path(__file__).resolve().parents[1]


def safe_id(value: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", value.strip())
    clean = re.sub(r"_+", "_", clean).strip("_").lower()
    if not clean:
        raise ValueError("Identifier cannot be empty")
    return clean


def load_env() -> None:
    env_path = ROOT / "secrets" / ".env"
    if not env_path.exists():
        raise SystemExit(f"Missing environment file: {env_path}")
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load_registry() -> dict[str, dict]:
    path = ROOT / "config" / "surveycto_forms.json"
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return dict(raw)


def write_dbt_sources(registry: dict[str, dict]) -> None:
    """Generate dbt source declarations from the active SurveyCTO registry."""
    target = ROOT / "dbt" / "research_platform" / "models" / "bronze" / "sources.yml"
    lines = [
        "version: 2",
        "",
        "sources:",
    ]
    for form_id, config in sorted(registry.items()):
        schema = safe_id(config["schema"])
        table_name = safe_id(form_id)
        lines.extend(
            [
                f"  - name: {schema}",
                f'    description: "Raw SurveyCTO submissions for {form_id}."',
                f"    schema: {schema}",
                "    tables:",
                f"      - name: {table_name}",
                f'        description: "Main survey table for {form_id}."',
                "        columns:",
                "          - name: submission_uuid",
                '            description: "Primary key: SurveyCTO KEY field."',
                "          - name: review_status",
                '            description: "SurveyCTO review console status."',
                "          - name: updated_at",
                '            description: "Timestamp of the last warehouse upsert."',
                "",
            ]
        )

    legacy_sources = {
        "client_unilever": {
            "table": "unilever_retail",
            "description": "Raw submissions for the legacy Unilever retail form.",
        },
        "internal": {
            "table": "internal_census",
            "description": "Raw submissions for the legacy internal census form.",
        },
    }
    generated_source_names = {safe_id(config["schema"]) for config in registry.values()}
    for source_name, info in legacy_sources.items():
        if source_name in generated_source_names:
            continue
        lines.extend(
            [
                f"  - name: {source_name}",
                f'    description: "{info["description"]}"',
                f"    schema: {source_name}",
                "    tables:",
                f"      - name: {info['table']}",
                f'        description: "{info["description"]}"',
                "        columns:",
                "          - name: submission_uuid",
                '            description: "Primary key: SurveyCTO KEY field."',
                "          - name: review_status",
                '            description: "SurveyCTO review console status."',
                "          - name: updated_at",
                '            description: "Timestamp of the last warehouse upsert."',
                "",
            ]
        )

    lines.extend(
        [
            "  - name: qc_system",
            '    description: "Platform metadata, QC flags, audit logs, and SLA trackers."',
            "    schema: qc_system",
            "    tables:",
            "      - name: qc_flags",
            '        description: "All QC flags raised by the quality control engine."',
            "      - name: sync_state",
            '        description: "Incremental sync cursor for each ETL pipeline."',
            "      - name: pipeline_sla",
            '        description: "Expected cadence and max-lag config per pipeline."',
            "      - name: registered_forms",
            '        description: "Active SurveyCTO form registry used by executive dashboards."',
            "      - name: project_state_quotas",
            '        description: "State-level project quota targets used for quota vs achievement dashboards."',
            "      - name: form_versions",
            '        description: "Detected SurveyCTO form schema versions and column manifests."',
            "      - name: enumerator_scores",
            '        description: "Per-enumerator quality scorecards computed by qc_engine."',
            "      - name: audit_log",
            '        description: "NDPR compliance access and system event audit trail."',
            "      - name: gps_boundaries",
            '        description: "Regional GPS bounding boxes used by the QC engine."',
            "",
        ]
    )
    target.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def write_dbt_models(registry: dict[str, dict]) -> None:
    """Create generic bronze and silver models for registered forms when absent."""
    bronze_dir = ROOT / "dbt" / "research_platform" / "models" / "bronze"
    silver_dir = ROOT / "dbt" / "research_platform" / "models" / "silver"
    manual_model_forms = {
        # Historical hand-written models already cover this source.
        "project_appraise",
    }

    for form_id, config in sorted(registry.items()):
        if form_id in manual_model_forms:
            continue
        schema = safe_id(config["schema"])
        table_name = safe_id(form_id)
        bronze_model = bronze_dir / f"stg_{table_name}.sql"
        silver_model = silver_dir / f"int_{table_name}.sql"

        if not bronze_model.exists():
            bronze_model.write_text(
                "\n".join(
                    [
                        "{{",
                        "  config(",
                        "    materialized = 'view',",
                        "    schema       = 'bronze'",
                        "  )",
                        "}}",
                        "",
                        f"select * from {{{{ source('{schema}', '{table_name}') }}}}",
                        "",
                    ]
                ),
                encoding="utf-8",
                newline="\n",
            )

        if not silver_model.exists():
            silver_model.write_text(
                "\n".join(
                    [
                        "{{",
                        "  config(",
                        "    materialized = 'view',",
                        "    schema       = 'silver'",
                        "  )",
                        "}}",
                        "",
                        "with source as (",
                        f"  select * from {{{{ ref('stg_{table_name}') }}}}",
                        ")",
                        "",
                        "select",
                        "  *,",
                        "  case",
                        "    when lower(review_status) in ('approved', 'rejected') then lower(review_status)",
                        "    else null",
                        "  end as review_status_clean,",
                        "  (lower(review_status) = 'approved') as is_approved,",
                        "  (lower(review_status) = 'rejected') as is_rejected",
                        "from source",
                        "",
                    ]
                ),
                encoding="utf-8",
                newline="\n",
            )


def db_kwargs() -> dict[str, object]:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5435")),
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }


def sync_forms() -> int:
    registry = load_registry()
    write_dbt_sources(registry)
    write_dbt_models(registry)
    with psycopg2.connect(**db_kwargs()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists qc_system.registered_forms (
                    form_id        text primary key,
                    client         text not null,
                    client_schema  text not null,
                    table_name     text not null,
                    active         boolean not null default true,
                    etl_deployment text,
                    qc_deployment  text,
                    registered_at  timestamptz not null default now(),
                    updated_at     timestamptz not null default now()
                )
                """
            )
            cur.execute(
                """
                create or replace function qc_system.table_row_count(
                    schema_name text,
                    table_name text
                ) returns bigint
                language plpgsql
                as $$
                declare
                    result bigint;
                begin
                    if to_regclass(format('%I.%I', schema_name, table_name)) is null then
                        return 0;
                    end if;

                    execute format('select count(*) from %I.%I', schema_name, table_name)
                    into result;

                    return coalesce(result, 0);
                end
                $$;
                """
            )
            cur.execute(
                """
                create table if not exists qc_system.pipeline_sla (
                    pipeline_name             text primary key,
                    expected_interval_minutes integer not null,
                    max_lag_minutes           integer not null,
                    owner                     text,
                    created_at                timestamptz default now()
                )
                """
            )
            cur.execute(
                """
                create table if not exists qc_system.project_state_quotas (
                    form_id       text not null,
                    client_schema text not null,
                    state_name    text not null,
                    quota_target  integer not null check (quota_target >= 0),
                    wave_name     text not null default 'default',
                    active        boolean not null default true,
                    notes         text,
                    created_at    timestamptz not null default now(),
                    updated_at    timestamptz not null default now(),
                    primary key (form_id, client_schema, state_name, wave_name)
                )
                """
            )
            cur.execute(
                """
                create index if not exists idx_project_state_quotas_active
                    on qc_system.project_state_quotas (active, form_id, state_name)
                """
            )
            for form_id, config in sorted(registry.items()):
                schema = safe_id(config["schema"])
                table_name = safe_id(form_id)
                cur.execute(f'create schema if not exists "{schema}"')
                cur.execute(f'grant usage on schema "{schema}" to etl_writer')
                cur.execute(f'grant usage on schema "{schema}" to analyst_reader')
                cur.execute(f'grant usage on schema "{schema}" to metabase_app')
                cur.execute(f'grant insert, update, select, delete on all tables in schema "{schema}" to etl_writer')
                cur.execute(f'grant select on all tables in schema "{schema}" to analyst_reader')
                cur.execute(f'grant select on all tables in schema "{schema}" to metabase_app')
                cur.execute(f'alter default privileges in schema "{schema}" grant insert, update, select, delete on tables to etl_writer')
                cur.execute(f'alter default privileges in schema "{schema}" grant select on tables to analyst_reader')
                cur.execute(f'alter default privileges in schema "{schema}" grant select on tables to metabase_app')
                cur.execute(
                    f"""
                    create table if not exists "{schema}"."{table_name}" (
                        submission_uuid text primary key,
                        review_status text,
                        updated_at timestamptz default now(),
                        deviceid text,
                        username text,
                        "SubmissionDate" text
                    )
                    """
                )
                cur.execute(
                    """
                    insert into qc_system.registered_forms (
                        form_id,
                        client,
                        client_schema,
                        table_name,
                        active,
                        etl_deployment,
                        qc_deployment,
                        updated_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, now())
                    on conflict (form_id) do update set
                        client         = excluded.client,
                        client_schema  = excluded.client_schema,
                        table_name     = excluded.table_name,
                        active         = excluded.active,
                        etl_deployment = excluded.etl_deployment,
                        qc_deployment  = excluded.qc_deployment,
                        updated_at     = now()
                    """,
                    (
                        form_id,
                        safe_id(config["client"]),
                        schema,
                        table_name,
                        config.get("active", True),
                        config.get("etl_deployment"),
                        config.get("qc_deployment"),
                    ),
                )
                if config.get("active", True):
                    cur.execute(
                        """
                        insert into qc_system.pipeline_sla (
                            pipeline_name,
                            expected_interval_minutes,
                            max_lag_minutes,
                            owner
                        )
                        values (%s, 1440, 1800, 'data_team')
                        on conflict (pipeline_name) do update set
                            expected_interval_minutes = excluded.expected_interval_minutes,
                            max_lag_minutes           = excluded.max_lag_minutes,
                            owner                     = excluded.owner
                        """,
                        (form_id,),
                    )
                else:
                    cur.execute(
                        "delete from qc_system.pipeline_sla where pipeline_name = %s",
                        (form_id,),
                    )
    return len(registry)


def main() -> int:
    load_env()
    count = sync_forms()
    active_count = sum(1 for config in load_registry().values() if config.get("active", True))
    print(
        f"Synced {count} registered SurveyCTO form(s) into qc_system.registered_forms "
        f"({active_count} active)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
