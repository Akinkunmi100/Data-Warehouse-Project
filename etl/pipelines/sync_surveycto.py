# ══════════════════════════════════════════
# Research Data Platform — SurveyCTO Incremental ETL
# ══════════════════════════════════════════

import os
import json
import hashlib
import requests
import uuid
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from prefect import flow, task
from prefect.logging import get_run_logger
from sqlalchemy import create_engine, text
import boto3
from botocore.client import Config

# Dynamic environmental loading relative to script path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, "..", "..", "secrets", ".env")
load_dotenv(ENV_PATH)

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
# Set DB_HOST to "localhost" because the script is running inside WSL, 
# and Docker container ports are exposed directly to WSL localhost.
DB_HOST = "localhost"  
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

SCTO_URL = os.getenv("SURVEYCTO_SERVER_URL")
SCTO_USER = os.getenv("SURVEYCTO_USERNAME")
SCTO_PASS = os.getenv("SURVEYCTO_PASSWORD")

# MinIO Credentials
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = "http://localhost:9000"

@task(retries=3, retry_delay_seconds=30, name="fetch-surveycto-submissions")
def fetch_submissions(form_id: str, last_sync: datetime) -> list:
    """Pulls new submissions from SurveyCTO API since last_sync."""
    logger = get_run_logger()
    
    # Construct base SurveyCTO V2 wide API endpoint
    url = f"{SCTO_URL}/api/v2/forms/data/wide/json/{form_id}"
    
    params = {}
    if last_sync:
        params["date"] = int(last_sync.timestamp())
        logger.info(f"Polling SurveyCTO incremental data since {last_sync} (epoch: {params['date']})")
    else:
        logger.info(f"No previous cursor found for form {form_id}. Performing full sync.")
        
    params["review_status"] = "approved|rejected"
    
    try:
        resp = requests.get(url, params=params, auth=(SCTO_USER, SCTO_PASS), timeout=60)
        resp.raise_for_status()
        
        data = resp.json()
        logger.info(f"Successfully retrieved {len(data)} records from SurveyCTO.")
        return data
    except Exception as e:
        logger.error(f"Error fetching from SurveyCTO API: {e}")
        raise

@task(name="verify-and-register-schema")
def check_schema(form_id: str, submissions: list, engine) -> bool:
    """Verifies schema consistency. If fields evolved, records audit event and halts."""
    logger = get_run_logger()
    if not submissions:
        return True
        
    incoming_cols = sorted(submissions[0].keys())
    col_hash = hashlib.sha256(json.dumps(incoming_cols).encode()).hexdigest()[:16]
    
    with engine.connect() as conn:
        query = text(
            "SELECT version_hash FROM qc_system.form_versions WHERE form_id=:fid ORDER BY detected_at DESC LIMIT 1"
        )
        existing = conn.execute(query, {"fid": form_id}).fetchone()
        
        if existing and existing[0] != col_hash:
            logger.error(f"⚠️ SCHEMA CHANGE DETECTED FOR FORM {form_id}! Halt pipeline.")
            audit_query = text(
                "INSERT INTO qc_system.audit_log(action, schema_name, detail) VALUES('schema_change', :fid, :detail)"
            )
            conn.execute(audit_query, {
                "fid": form_id, 
                "detail": json.dumps({"old_hash": existing[0], "new_hash": col_hash, "columns": incoming_cols})
            })
            conn.commit()
            raise ValueError(f"Schema drift for form {form_id}. Halting pipeline for administrative intervention.")
            
        if not existing:
            logger.info(f"Registering base schema for form {form_id} (hash: {col_hash}).")
            register_query = text(
                "INSERT INTO qc_system.form_versions (form_id, version_hash, column_manifest) VALUES (:fid, :hash, :manifest)"
            )
            conn.execute(register_query, {
                "fid": form_id,
                "hash": col_hash,
                "manifest": json.dumps(incoming_cols)
            })
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
    key = f"{client}/{form_id}/{timestamp}/raw.json"
    
    logger.info(f"Writing raw payload to raw-bronze S3 at '{key}'")
    s3.put_object(
        Bucket='raw-bronze',
        Key=key,
        Body=json.dumps(submissions).encode('utf-8')
    )

@task(name="flatten-and-upsert-postgres")
def upsert_submissions(form_id: str, client_schema: str, submissions: list, engine):
    """Processes, flattens, and loads nested/flat structures into warehouse."""
    logger = get_run_logger()
    if not submissions:
        return
        
    flat_rows = []
    repeat_groups = {}
    
    for sub in submissions:
        scalar_row = {k: v for k, v in sub.items() if not isinstance(v, list)}
        flat_rows.append(scalar_row)
        
        for k, v in sub.items():
            if isinstance(v, list):
                repeat_groups.setdefault(k, [])
                for idx, item in enumerate(v):
                    item['_parent_uuid'] = sub['KEY']
                    item['_repeat_index'] = idx
                    repeat_groups[k].append(item)
                    
    conn = engine.connect()
    
    df_base = pd.DataFrame(flat_rows)
    df_base.rename(columns={'KEY': 'submission_uuid'}, inplace=True)
    df_base['review_status'] = df_base.get('review_status', 'unknown')
    
    target_table_name = form_id.replace('-', '_')
    target_table_fq = f"{client_schema}.{target_table_name}"
    
    stage_uuid = uuid.uuid4().hex[:12]
    stage_table_name = f"_stage_{stage_uuid}"
    
    logger.info(f"Loading {len(df_base)} base records into temporary stage 'qc_system.{stage_table_name}'")
    df_base.to_sql(stage_table_name, engine, schema='qc_system', if_exists='replace', index=False)
    
    try:
        columns_sql = ", ".join([f'"{col}"' for col in df_base.columns])
        
        check_table = conn.execute(text(
            f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='{client_schema}' AND table_name='{target_table_name}')"
        )).fetchone()[0]
        
        if not check_table:
            logger.info(f"Target table '{target_table_fq}' does not exist. Creating dynamically.")
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
                
        upsert_sql = f"""
            INSERT INTO {target_table_fq} ({columns_sql})
            SELECT {columns_sql} FROM qc_system.{stage_table_name}
            ON CONFLICT (submission_uuid) DO UPDATE SET {update_set_sql}
        """
        
        logger.info(f"Executing base upsert onto {target_table_fq}")
        conn.execute(text(upsert_sql))
        conn.commit()
        
    finally:
        conn.execute(text(f"DROP TABLE IF EXISTS qc_system.{stage_table_name}"))
        conn.commit()
        
    for group_name, rows in repeat_groups.items():
        child_table_name = f"{target_table_name}_{group_name.replace('-', '_')}"
        child_table_fq = f"{client_schema}.{child_table_name}"
        
        df_child = pd.DataFrame(rows)
        stage_child_name = f"_stage_child_{stage_uuid}_{group_name[:8]}"
        
        logger.info(f"Processing repeat group '{group_name}' ({len(df_child)} rows) via staging '{stage_child_name}'")
        df_child.to_sql(stage_child_name, engine, schema='qc_system', if_exists='replace', index=False)
        
        try:
            check_child = conn.execute(text(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='{client_schema}' AND table_name='{child_table_name}')"
            )).fetchone()[0]
            
            if not check_child:
                logger.info(f"Child table '{child_table_fq}' does not exist. Creating dynamically.")
                conn.execute(text(f"CREATE TABLE {child_table_fq} AS SELECT * FROM qc_system.{stage_child_name} WITH NO DATA"))
                conn.execute(text(f"ALTER TABLE {child_table_fq} ADD COLUMN IF NOT EXISTS _parent_uuid TEXT"))
                conn.execute(text(f"ALTER TABLE {child_table_fq} ADD COLUMN IF NOT EXISTS _repeat_index INT"))
                conn.execute(text(f"ALTER TABLE {child_table_fq} ADD PRIMARY KEY (_parent_uuid, _repeat_index)"))
                conn.commit()
                
            child_columns = ", ".join([f'"{col}"' for col in df_child.columns])
            
            upsert_child_sql = f"""
                INSERT INTO {child_table_fq} ({child_columns})
                SELECT {child_columns} FROM qc_system.{stage_child_name}
                ON CONFLICT (_parent_uuid, _repeat_index) DO NOTHING
            """
            conn.execute(text(upsert_child_sql))
            conn.commit()
        finally:
            conn.execute(text(f"DROP TABLE IF EXISTS qc_system.{stage_child_name}"))
            conn.commit()
            
    conn.close()

@flow(name="surveycto-ingestion-orchestrator", log_prints=True)
def run_etl(form_id: str, client: str, client_schema: str):
    """Master workflow governing SurveyCTO data integration."""
    logger = get_run_logger()
    logger.info(f"🚀 Initializing SurveyCTO ingestion workflow for Form: {form_id} Client: {client}")
    
    engine = create_engine(DB_URL)
    
    last_sync = None
    with engine.connect() as conn:
        sync_state_row = conn.execute(
            text("SELECT last_successful_sync FROM qc_system.sync_state WHERE pipeline_name = :n"),
            {"n": form_id}
        ).fetchone()
        if sync_state_row:
            last_sync = sync_state_row[0]
            
    try:
        submissions = fetch_submissions(form_id, last_sync)
        
        if not submissions:
            logger.info("No new records since last sync cursor. Ingestion skipped.")
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO qc_system.sync_state (pipeline_name, last_run_status, updated_at)
                    VALUES (:n, 'success_no_data', NOW())
                    ON CONFLICT (pipeline_name) DO UPDATE SET
                        last_run_status = 'success_no_data',
                        updated_at = NOW()
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
                    last_successful_sync = NOW(),
                    last_run_status = 'success',
                    updated_at = NOW()
            """), {"n": form_id})
            conn.commit()
            
        logger.info(f"✨ Ingestion complete. Incremental sync cursor advanced for {form_id}.")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO qc_system.failed_payloads (form_id, client_schema, raw_payload, error_message)
                    VALUES (:f, :s, :p::jsonb, :e)
                """), {
                    "f": form_id,
                    "s": client_schema,
                    "p": json.dumps(submissions) if 'submissions' in locals() and submissions else "[]",
                    "e": str(e)
                })
                conn.execute(text("""
                    INSERT INTO qc_system.sync_state (pipeline_name, last_run_status, updated_at)
                    VALUES (:n, 'failed', NOW())
                    ON CONFLICT (pipeline_name) DO UPDATE SET
                        last_run_status = 'failed',
                        updated_at = NOW()
                """), {"n": form_id})
                conn.commit()
        except Exception as dlq_err:
            logger.error(f"Failed to record failure state to database: {dlq_err}")
        raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 3:
        fid = sys.argv[1]
        clt = sys.argv[2]
        sch = sys.argv[3]
        run_etl(fid, clt, sch)
    else:
        run_etl.serve(
            name="surveycto-nightly-poll",
            cron="0 1 * * *",
            parameters={"form_id": "brand-tracker", "client": "client_mtn", "client_schema": "client_mtn"}
        )
