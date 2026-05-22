# ══════════════════════════════════════════
# Research Data Platform — SurveyCTO Incremental ETL
# ══════════════════════════════════════════

import os
import sys

from dotenv import load_dotenv

# ── Path setup (must come before any project-local imports) ──────────────────
# When this script runs as __main__ (e.g. via systemd or direct invocation),
# Python inserts the script's own directory (etl/pipelines/) onto sys.path[0],
# NOT the project root. That makes `import etl.utils` fail with ModuleNotFoundError.
# We explicitly add the project root so the etl package is always importable.
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

ENV_PATH = os.path.join(SCRIPT_DIR, "..", "..", "secrets", ".env")
load_dotenv(ENV_PATH)

import json
import hashlib
import requests
import uuid
import pandas as pd
from datetime import datetime, timezone
from prefect import flow, task, serve
from prefect.logging import get_run_logger
from sqlalchemy import create_engine, text
import boto3
from botocore.client import Config

from etl.utils import safe_id as _safe_id, build_db_url

# FIX: was building DB_URL inline without URL-encoding the password.
# Passwords containing @, #, %, +, / silently break SQLAlchemy's URL parser.
# Now delegates to etl.utils.build_db_url() which uses urllib.parse.quote_plus.
DB_URL = build_db_url()

SCTO_URL  = os.getenv("SURVEYCTO_SERVER_URL")
SCTO_USER = os.getenv("SURVEYCTO_USERNAME")
SCTO_PASS = os.getenv("SURVEYCTO_PASSWORD")

MINIO_USER     = os.getenv("MINIO_ROOT_USER")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = "http://localhost:9000"


@task(retries=3, retry_delay_seconds=30, name="fetch-surveycto-submissions")
def fetch_submissions(form_id: str, last_sync: datetime) -> list:
    """Pulls new submissions from SurveyCTO API since last_sync."""
    logger = get_run_logger()

    url    = SCTO_URL + "/api/v2/forms/data/wide/json/" + str(form_id)
    params = {}

    if last_sync:
        params["date"] = int(last_sync.timestamp())
        logger.info(f"Polling SurveyCTO incremental data since {last_sync} (epoch: {params['date']})")
    else:
        logger.info(f"No previous cursor found for {form_id}. Performing full sync.")
        params["date"] = 0

    params["review_status"] = "approved|rejected"

    try:
        resp = requests.get(url, params=params, auth=(SCTO_USER, SCTO_PASS), timeout=60)

        if resp.status_code == 400:
            logger.warning(f"SurveyCTO returned 400: {resp.text}. Retrying without review_status filter...")
            params.pop("review_status", None)
            resp = requests.get(url, params=params, auth=(SCTO_USER, SCTO_PASS), timeout=60)

        if resp.status_code != 200:
            logger.error(f"SurveyCTO API failed {resp.status_code}: {resp.text}")

        resp.raise_for_status()
        data = resp.json()
        logger.info(f"Retrieved {len(data)} records from SurveyCTO.")
        return data
    except Exception as e:
        logger.error(f"Error fetching from SurveyCTO: {e}")
        raise


@task(name="verify-and-register-schema")
def check_schema(form_id: str, submissions: list, engine) -> bool:
    """Verifies schema consistency. If fields evolved, records audit event and halts."""
    logger = get_run_logger()
    if not submissions:
        return True

    incoming_cols = sorted(submissions[0].keys())
    col_hash      = hashlib.sha256(json.dumps(incoming_cols).encode()).hexdigest()[:16]

    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT version_hash FROM qc_system.form_versions WHERE form_id=:fid ORDER BY detected_at DESC LIMIT 1"),
            {"fid": form_id}
        ).fetchone()

        if existing and existing[0] != col_hash:
            logger.error(f"⚠️ SCHEMA CHANGE DETECTED for {form_id}! Halting pipeline.")
            conn.execute(
                text("INSERT INTO qc_system.audit_log(action, schema_name, detail) VALUES('schema_change', :fid, :detail)"),
                {"fid": form_id, "detail": json.dumps({"old_hash": existing[0], "new_hash": col_hash, "columns": incoming_cols})}
            )
            conn.commit()
            raise ValueError(f"Schema drift for form {form_id}. Halting for administrative intervention.")

        if not existing:
            logger.info(f"Registering base schema for {form_id} (hash: {col_hash}).")
            conn.execute(
                text("INSERT INTO qc_system.form_versions (form_id, version_hash, column_manifest) VALUES (:fid, :hash, :manifest)"),
                {"fid": form_id, "hash": col_hash, "manifest": json.dumps(incoming_cols)}
            )
            conn.commit()

    return True


@task(name="save-raw-bronze-layer")
def save_raw_to_minio(form_id: str, client: str, submissions: list):
    """Stores raw immutable JSON payloads in MinIO S3 bucket."""
    logger = get_run_logger()
    if not submissions:
        return

    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
        config=Config(signature_version='s3v4')
    )

    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H%M%S')
    key       = f"{client}/{form_id}/{timestamp}/raw.json"

    logger.info(f"Writing raw payload to raw-bronze at '{key}'")
    s3.put_object(Bucket='raw-bronze', Key=key, Body=json.dumps(submissions).encode('utf-8'))


@task(name="flatten-and-upsert-postgres")
def upsert_submissions(form_id: str, client_schema: str, submissions: list, engine):
    """Processes, flattens, and loads nested/flat structures into warehouse."""
    logger = get_run_logger()
    if not submissions:
        return

    safe_form_id       = _safe_id(form_id)
    safe_client_schema = _safe_id(client_schema)

    flat_rows     = []
    repeat_groups = {}

    for sub in submissions:
        if 'KEY' not in sub:
            logger.warning(f"Skipping submission missing 'KEY': {str(sub)[:200]}")
            continue
        scalar_row = {k: v for k, v in sub.items() if not isinstance(v, list)}
        flat_rows.append(scalar_row)
        for k, v in sub.items():
            if isinstance(v, list):
                repeat_groups.setdefault(k, [])
                for idx, item in enumerate(v):
                    item['_parent_uuid']  = sub['KEY']
                    item['_repeat_index'] = idx
                    repeat_groups[k].append(item)

    if not flat_rows:
        logger.warning("No valid submissions after filtering. Aborting upsert.")
        return

    with engine.connect() as conn:
        df_base = pd.DataFrame(flat_rows)
        df_base.rename(columns={'KEY': 'submission_uuid'}, inplace=True)
        if 'review_status' not in df_base.columns:
            df_base['review_status'] = 'unknown'

        target_table_name = safe_form_id
        target_table_fq   = f"{safe_client_schema}.{target_table_name}"
        stage_uuid        = uuid.uuid4().hex[:12]
        stage_table_name  = f"_stage_{stage_uuid}"

        logger.info(f"Staging {len(df_base)} records in qc_system.{stage_table_name}")
        df_base.to_sql(stage_table_name, conn, schema='qc_system', if_exists='replace', index=False)

        try:
            columns_sql = ", ".join([f'"{col}"' for col in df_base.columns])

            check_table = conn.execute(text(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='{safe_client_schema}' AND table_name='{target_table_name}')"
            )).fetchone()[0]

            if not check_table:
                logger.info(f"Creating target table '{target_table_fq}'.")
                conn.execute(text(f"CREATE TABLE {target_table_fq} AS SELECT * FROM qc_system.{stage_table_name} WITH NO DATA"))
                conn.execute(text(f"ALTER TABLE {target_table_fq} ADD PRIMARY KEY (submission_uuid)"))
                conn.commit()

            update_set_sql = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in df_base.columns if col not in ['submission_uuid', 'KEY']])
            if not update_set_sql:
                update_set_sql = '"updated_at" = NOW()'
            else:
                if 'updated_at' not in df_base.columns:
                    try:
                        conn.execute(text(f"ALTER TABLE {target_table_fq} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()"))
                        conn.commit()
                    except Exception:
                        pass
                    update_set_sql += ', "updated_at" = NOW()'

            conn.execute(text(f"""
                INSERT INTO {target_table_fq} ({columns_sql})
                SELECT {columns_sql} FROM qc_system.{stage_table_name}
                ON CONFLICT (submission_uuid) DO UPDATE SET {update_set_sql}
            """))
            conn.commit()

        finally:
            conn.execute(text(f"DROP TABLE IF EXISTS qc_system.{stage_table_name}"))
            conn.commit()

        for group_name, rows in repeat_groups.items():
            safe_group       = _safe_id(group_name)
            child_table_name = f"{target_table_name}_{safe_group}"
            child_table_fq   = f"{safe_client_schema}.{child_table_name}"
            df_child         = pd.DataFrame(rows)
            stage_child_name = f"_stage_child_{stage_uuid}_{safe_group[:8]}"

            logger.info(f"Processing repeat group '{group_name}' ({len(df_child)} rows)")
            df_child.to_sql(stage_child_name, conn, schema='qc_system', if_exists='replace', index=False)

            try:
                check_child = conn.execute(text(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='{safe_client_schema}' AND table_name='{child_table_name}')"
                )).fetchone()[0]

                if not check_child:
                    logger.info(f"Creating child table '{child_table_fq}'.")
                    conn.execute(text(f"CREATE TABLE {child_table_fq} AS SELECT * FROM qc_system.{stage_child_name} WITH NO DATA"))
                    conn.execute(text(f"ALTER TABLE {child_table_fq} ADD COLUMN IF NOT EXISTS _parent_uuid TEXT"))
                    conn.execute(text(f"ALTER TABLE {child_table_fq} ADD COLUMN IF NOT EXISTS _repeat_index INT"))
                    conn.execute(text(f"ALTER TABLE {child_table_fq} ADD PRIMARY KEY (_parent_uuid, _repeat_index)"))
                    conn.commit()

                child_columns = ", ".join([f'"{col}"' for col in df_child.columns])
                conn.execute(text(f"""
                    INSERT INTO {child_table_fq} ({child_columns})
                    SELECT {child_columns} FROM qc_system.{stage_child_name}
                    ON CONFLICT (_parent_uuid, _repeat_index) DO NOTHING
                """))
                conn.commit()
            finally:
                conn.execute(text(f"DROP TABLE IF EXISTS qc_system.{stage_child_name}"))
                conn.commit()


@task(retries=1, name="execute-dbt-models")
def run_dbt_models():
    """Runs dbt build to update analytical models. Failures are warnings — do not reraise."""
    logger  = get_run_logger()
    dbt_dir = os.path.join(PROJECT_ROOT, "dbt", "research_platform")
    logger.info(f"Executing dbt build in {dbt_dir}")

    import subprocess
    result = subprocess.run(["dbt", "build"], cwd=dbt_dir, capture_output=True, text=True)

    if result.returncode != 0:
        logger.warning("⚠️  dbt build returned non-zero — analytical layer may be stale.\n"
                       + result.stdout + "\n" + result.stderr)
        # Do NOT raise: the data upsert already succeeded and the sync cursor is advanced.
    else:
        logger.info("✅ dbt build complete.\n" + result.stdout)


@flow(name="surveycto-ingestion-orchestrator", log_prints=True)
def run_etl(form_id: str, client: str, client_schema: str):
    """Master workflow governing SurveyCTO data integration."""
    logger = get_run_logger()
    logger.info(f"🚀 Initialising ETL for form={form_id} client={client}")

    engine    = create_engine(DB_URL)
    last_sync = None

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT last_successful_sync FROM qc_system.sync_state WHERE pipeline_name = :n"),
            {"n": form_id}
        ).fetchone()
        if row:
            last_sync = row[0]

    try:
        submissions = fetch_submissions(form_id, last_sync)

        if not submissions:
            logger.info("No new records. Ingestion skipped.")
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO qc_system.sync_state (pipeline_name, last_run_status, updated_at)
                    VALUES (:n, 'success_no_data', NOW())
                    ON CONFLICT (pipeline_name) DO UPDATE SET last_run_status='success_no_data', updated_at=NOW()
                """), {"n": form_id})
                conn.commit()
            return

        check_schema(form_id, submissions, engine)
        save_raw_to_minio(form_id, client, submissions)
        upsert_submissions(form_id, client_schema, submissions, engine)

        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO qc_system.sync_state (pipeline_name, last_successful_sync, last_run_status, updated_at)
                VALUES (:n, NOW(), 'success', NOW())
                ON CONFLICT (pipeline_name) DO UPDATE SET
                    last_successful_sync=NOW(), last_run_status='success', updated_at=NOW()
            """), {"n": form_id})
            conn.commit()

        logger.info(f"✨ Ingestion complete for {form_id}.")
        # Trigger QC engine immediately after a successful ingest so that
        # webhook-triggered runs get flagged in real time — not just at 1:30am.
        from etl.qc.qc_engine import run_qc
        run_qc(form_id=form_id, client_schema=client_schema)
        run_dbt_models()

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO qc_system.failed_payloads (form_id, client_schema, raw_payload, error_message)
                    VALUES (:f, :s, :p::jsonb, :e)
                """), {
                    "f": form_id, "s": client_schema,
                    "p": json.dumps(submissions) if 'submissions' in locals() and submissions else "[]",
                    "e": str(e)
                })
                conn.execute(text("""
                    INSERT INTO qc_system.sync_state (pipeline_name, last_run_status, updated_at)
                    VALUES (:n, 'failed', NOW())
                    ON CONFLICT (pipeline_name) DO UPDATE SET last_run_status='failed', updated_at=NOW()
                """), {"n": form_id})
                conn.commit()
        except Exception as dlq_err:
            logger.error(f"Failed to write failure state to DB: {dlq_err}")
        raise


if __name__ == "__main__":
    if len(sys.argv) > 3:
        run_etl(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        dep_mtn = run_etl.to_deployment(
            name="surveycto-nightly-mtn",
            cron="0 1 * * *",
            parameters={"form_id": "project_appraise", "client": "client_mtn", "client_schema": "client_mtn"}
        )
        dep_unilever = run_etl.to_deployment(
            name="surveycto-nightly-unilever",
            cron="0 1 * * *",
            parameters={"form_id": "unilever-retail", "client": "client_unilever", "client_schema": "client_unilever"}
        )
        dep_internal = run_etl.to_deployment(
            name="surveycto-nightly-internal",
            cron="0 1 * * *",
            parameters={"form_id": "internal-census", "client": "internal", "client_schema": "internal"}
        )
        serve(dep_mtn, dep_unilever, dep_internal)
