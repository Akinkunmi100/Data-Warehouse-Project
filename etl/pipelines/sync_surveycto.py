# ══════════════════════════════════════════
# Research Data Platform — SurveyCTO Incremental ETL
# ══════════════════════════════════════════

import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

# ── Path setup (must come before any project-local imports) ──────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

ENV_PATH = os.path.join(PROJECT_ROOT, "secrets", ".env")
load_dotenv(ENV_PATH)

import json
import hashlib
import requests
import uuid
import pandas as pd
from datetime import datetime, timezone, timedelta
from prefect import flow, task, serve
from prefect.cache_policies import NO_CACHE
from prefect.logging import get_run_logger
from sqlalchemy import create_engine, text
import boto3
from botocore.client import Config

from etl.surveycto_registry import get_form_config, load_form_registry
from etl.utils import safe_id as _safe_id, build_db_url

DB_URL = build_db_url()

SCTO_URL  = os.getenv("SURVEYCTO_SERVER_URL", "").rstrip("/")
SCTO_USER = os.getenv("SURVEYCTO_USERNAME", "")
SCTO_PASS = os.getenv("SURVEYCTO_PASSWORD", "")
SCTO_PRIVATE_KEY_PATH = os.getenv("SURVEYCTO_PRIVATE_KEY_PATH", "").strip()
SCTO_REVIEW_STATUS = os.getenv("SURVEYCTO_REVIEW_STATUS", "").strip()

# Timeout in seconds for SurveyCTO API calls.
# Increase via SURVEYCTO_TIMEOUT env var if large form exports exceed 120 s.
SCTO_TIMEOUT = int(os.getenv("SURVEYCTO_TIMEOUT", "120"))
SCTO_417_WAIT_SECONDS = int(os.getenv("SURVEYCTO_417_WAIT_SECONDS", "305"))
SCTO_SYNC_OVERLAP_MINUTES = int(os.getenv("SURVEYCTO_SYNC_OVERLAP_MINUTES", "10"))

MINIO_USER     = os.getenv("MINIO_ROOT_USER")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = "http://localhost:9000"


def _quote_ident(identifier: str) -> str:
    """Quote a PostgreSQL identifier."""
    return '"' + str(identifier).replace('"', '""') + '"'


def _resolve_private_key_path(configured_path: str) -> str:
    """Resolve SurveyCTO private-key paths across Windows/WSL workspace moves."""
    if not configured_path:
        return ""

    candidates = [Path(configured_path)]
    original = Path(configured_path)
    secrets_dir = Path(PROJECT_ROOT) / "secrets"
    candidates.append(secrets_dir / original.name)
    if original.suffix.lower() != ".pem":
        candidates.append(Path(str(configured_path) + ".pem"))
        candidates.append(secrets_dir / f"{original.name}.pem")

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return configured_path


SCTO_PRIVATE_KEY_PATH = _resolve_private_key_path(SCTO_PRIVATE_KEY_PATH)


def _surveycto_request(url: str, params: dict):
    """Fetch SurveyCTO data, posting a private key when one is configured."""
    if SCTO_PRIVATE_KEY_PATH:
        if not os.path.exists(SCTO_PRIVATE_KEY_PATH):
            raise RuntimeError(
                "SURVEYCTO_PRIVATE_KEY_PATH is set, but the file does not exist: "
                f"{SCTO_PRIVATE_KEY_PATH}"
            )
        with open(SCTO_PRIVATE_KEY_PATH, "rb") as key_file:
            return requests.post(
                url,
                params=params,
                files={"private_key": key_file},
                auth=(SCTO_USER, SCTO_PASS),
                timeout=SCTO_TIMEOUT,
            )

    return requests.get(
        url,
        params=params,
        auth=(SCTO_USER, SCTO_PASS),
        timeout=SCTO_TIMEOUT,
    )


def _classify_scto_error(status_code: int, form_id: str, url: str) -> str:
    """Return an actionable error message for each known SurveyCTO HTTP status."""
    if status_code == 401:
        return (
            f"HTTP 401 Unauthorized — credentials rejected by {SCTO_URL}.\n"
            f"  Check SURVEYCTO_USERNAME ('{SCTO_USER}') and SURVEYCTO_PASSWORD in secrets/.env.\n"
            f"  Run: python3 scripts/test_surveycto.py"
        )
    if status_code == 403:
        return (
            f"HTTP 403 Forbidden — user '{SCTO_USER}' does not have API access to form '{form_id}'.\n"
            f"  In SurveyCTO: Admin → Users → edit user → enable 'API access'.\n"
            f"  Also verify the user has 'Download data' permission for this form."
        )
    if status_code == 404:
        return (
            f"HTTP 404 Not Found — form '{form_id}' does not exist on {SCTO_URL}.\n"
            f"  The form ID in your deployment parameters must match exactly.\n"
            f"  Run: python3 scripts/test_surveycto.py --list-forms"
        )
    if status_code == 429:
        return "HTTP 429 Too Many Requests — SurveyCTO rate limit. Will retry (retries=3)."
    return f"HTTP {status_code} from {url}"


@task(retries=3, retry_delay_seconds=60, name="fetch-surveycto-submissions")
def fetch_submissions(form_id: str, last_sync: datetime) -> list:
    """
    Pulls new or updated submissions from SurveyCTO API v2 (wide JSON format).

    Parameters
    ----------
    form_id   : SurveyCTO form ID (must match the ID on the server exactly).
    last_sync : Datetime of the last successful pull; None triggers a full sync.

    Returns
    -------
    List of submission dicts. Each dict always contains a 'KEY' field.

    Incremental behaviour
    ---------------------
    SurveyCTO API v2 `date` parameter accepts milliseconds since the Unix epoch.
    We pass  int(last_sync.timestamp() * 1000)  for incremental pulls.
    For a full sync (last_sync is None) we pass date=0  (epoch start → all data).

    FIX: previous version sent  int(last_sync.timestamp())  (seconds).
    SurveyCTO interpreted e.g. 1 748 100 000 seconds as 1 748 100 000 ms = Jan 1970,
    so EVERY incremental run re-fetched ALL data regardless of the cursor.
    The fix multiplies by 1000 to convert to the correct millisecond representation.
    """
    logger = get_run_logger()

    if not SCTO_URL:
        raise RuntimeError("SURVEYCTO_SERVER_URL is not set in secrets/.env")
    if not SCTO_USER or not SCTO_PASS:
        raise RuntimeError("SURVEYCTO_USERNAME or SURVEYCTO_PASSWORD is not set in secrets/.env")

    url = f"{SCTO_URL}/api/v2/forms/data/wide/json/{form_id}"

    # ── Build query parameters ────────────────────────────────────────────────
    if last_sync:
        effective_last_sync = last_sync - timedelta(minutes=SCTO_SYNC_OVERLAP_MINUTES)
        # FIX: multiply by 1000 — SurveyCTO expects MILLISECONDS since epoch
        date_ms = int(effective_last_sync.timestamp() * 1000)
        logger.info(
            f"Incremental pull for '{form_id}' since {effective_last_sync.isoformat()} "
            f"(stored cursor: {last_sync.isoformat()}, overlap: {SCTO_SYNC_OVERLAP_MINUTES} min) "
            f"(epoch ms: {date_ms})"
        )
    else:
        date_ms = 0
        logger.info(f"Full sync for '{form_id}' (no previous cursor — fetching all data)")

    # Blank SURVEYCTO_REVIEW_STATUS means export all statuses. Set it to values
    # like approved|rejected if the dashboard should exclude pending records.
    params_without_filter = {"date": date_ms}
    review_status = SCTO_REVIEW_STATUS.strip()
    if review_status and review_status.lower() not in {"all", "*"}:
        params_with_filter = {"date": date_ms, "review_status": review_status}
        request_attempts = [params_with_filter, params_without_filter]
    else:
        params_with_filter = None
        request_attempts = [params_without_filter]

    for attempt, params in enumerate(request_attempts, start=1):
        has_filter = "review_status" in params
        label = "with review_status filter" if has_filter else "without review_status filter"
        method = "POST" if SCTO_PRIVATE_KEY_PATH else "GET"
        logger.info(f"Attempt {attempt}: {method} {url}  [{label}]")

        try:
            resp = _surveycto_request(url, params)
        except requests.exceptions.Timeout:
            raise RuntimeError(
                f"SurveyCTO API timed out after {SCTO_TIMEOUT} s for form '{form_id}'.\n"
                f"  Set SURVEYCTO_TIMEOUT=300 in secrets/.env for large exports."
            )
        except requests.exceptions.ConnectionError as e:
            raise RuntimeError(f"Cannot reach SurveyCTO server at {SCTO_URL}: {e}")

        # On first attempt a 400 means review_status filter not supported → retry
        if resp.status_code == 400 and has_filter:
            logger.warning(
                f"HTTP 400 with review_status filter — server may not support it. "
                f"Retrying without filter..."
            )
            continue

        if resp.status_code == 417 and not last_sync and date_ms == 0:
            logger.warning(
                f"SurveyCTO returned HTTP 417 for a full sync of '{form_id}'. "
                f"Waiting {SCTO_417_WAIT_SECONDS} seconds, then retrying the "
                "same full export so the whole dataset is preserved."
            )
            time.sleep(SCTO_417_WAIT_SECONDS)
            resp = _surveycto_request(url, params)

        if resp.status_code != 200:
            raise RuntimeError(_classify_scto_error(resp.status_code, form_id, url))

        break  # success

    # ── Parse response ────────────────────────────────────────────────────────
    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(
            f"SurveyCTO response for '{form_id}' is not valid JSON: {e}\n"
            f"Raw (first 500 chars): {resp.text[:500]}"
        )

    if not isinstance(data, list):
        raise RuntimeError(
            f"SurveyCTO API returned unexpected type '{type(data).__name__}' for '{form_id}'. "
            f"Expected a JSON array. Response: {str(data)[:300]}"
        )

    logger.info(f"Retrieved {len(data)} submission(s) from SurveyCTO for '{form_id}'.")

    # Warn if KEY is missing — this is required for upsert
    if data and "KEY" not in data[0]:
        raise RuntimeError(
            f"SurveyCTO response for '{form_id}' is missing the 'KEY' field.\n"
            f"Ensure you are using the wide-format JSON endpoint "
            f"(/api/v2/forms/data/wide/json/...).\n"
            f"Columns received: {list(data[0].keys())[:20]}"
        )

    return data


@task(name="verify-and-register-schema", cache_policy=NO_CACHE)
def _legacy_check_schema(form_id: str, submissions: list, engine) -> bool:
    """
    Detects schema drift by hashing the column list of incoming data.

    On the first sync the schema is registered as the baseline.
    On subsequent syncs, if the column set changes (form fields added/removed),
    the pipeline halts and logs an audit event for administrator review.
    This prevents silently loading mismatched data into the warehouse.
    """
    logger = get_run_logger()
    if not submissions:
        return True

    incoming_cols = sorted({key for sub in submissions for key in sub.keys()})
    col_hash      = hashlib.sha256(json.dumps(incoming_cols).encode()).hexdigest()[:16]

    with engine.connect() as conn:
        existing = conn.execute(
            text(
                "SELECT version_hash, column_manifest FROM qc_system.form_versions "
                "WHERE form_id=:fid ORDER BY detected_at DESC LIMIT 1"
            ),
            {"fid": form_id}
        ).fetchone()

        if existing and existing[0] != col_hash:
            logger.error(
                f"⚠️ SCHEMA CHANGE DETECTED for '{form_id}'!\n"
                f"  Previous hash: {existing[0]}\n"
                f"  New hash:      {col_hash}\n"
                f"  Columns received: {incoming_cols}\n"
                f"  Halting pipeline. Review and reset form_versions if the change is expected."
            )
            conn.execute(
                text(
                    "INSERT INTO qc_system.audit_log(action, schema_name, detail) "
                    "VALUES('schema_change', :fid, :detail)"
                ),
                {
                    "fid":    form_id,
                    "detail": json.dumps({
                        "old_hash":  existing[0],
                        "new_hash":  col_hash,
                        "columns":   incoming_cols,
                    }),
                }
            )
            conn.commit()
            raise ValueError(
                f"Schema drift for form '{form_id}'. "
                f"Halting for administrator review. "
                f"To accept the new schema: "
                f"DELETE FROM qc_system.form_versions WHERE form_id='{form_id}';"
            )

        if not existing:
            logger.info(
                f"Registering baseline schema for '{form_id}' "
                f"(hash: {col_hash}, {len(incoming_cols)} columns)"
            )
            conn.execute(
                text(
                    "INSERT INTO qc_system.form_versions "
                    "(form_id, version_hash, column_manifest) "
                    "VALUES (:fid, :hash, :manifest)"
                ),
                {
                    "fid":      form_id,
                    "hash":     col_hash,
                    "manifest": json.dumps(incoming_cols),
                }
            )
            conn.commit()

    return True


@task(name="verify-and-register-schema", cache_policy=NO_CACHE)
def check_schema(form_id: str, submissions: list, engine) -> bool:
    """Register schema versions while allowing normal incremental subsets."""
    logger = get_run_logger()
    if not submissions:
        return True

    incoming_cols = sorted({key for sub in submissions for key in sub.keys()})
    col_hash = hashlib.sha256(json.dumps(incoming_cols).encode()).hexdigest()[:16]

    with engine.connect() as conn:
        existing = conn.execute(
            text(
                "SELECT version_hash, column_manifest FROM qc_system.form_versions "
                "WHERE form_id=:fid ORDER BY detected_at DESC LIMIT 1"
            ),
            {"fid": form_id},
        ).fetchone()

        if existing and existing[0] != col_hash:
            try:
                previous_cols = set(json.loads(existing[1] or "[]"))
            except Exception:
                previous_cols = set()

            incoming_set = set(incoming_cols)
            new_cols = sorted(incoming_set - previous_cols)
            missing_cols = sorted(previous_cols - incoming_set)

            if previous_cols and not new_cols:
                logger.warning(
                    f"SurveyCTO incremental batch for '{form_id}' returned a subset "
                    f"of known columns ({len(missing_cols)} omitted). Continuing."
                )
                return True

            merged_cols = sorted(previous_cols | incoming_set) if previous_cols else incoming_cols
            merged_hash = hashlib.sha256(json.dumps(merged_cols).encode()).hexdigest()[:16]
            logger.warning(
                f"Schema extension detected for '{form_id}'. Continuing with controlled evolution.\n"
                f"  Previous hash: {existing[0]}\n"
                f"  New hash:      {merged_hash}\n"
                f"  Added columns: {new_cols}\n"
                f"  Omitted columns in this batch: {missing_cols}"
            )
            conn.execute(
                text(
                    "INSERT INTO qc_system.audit_log(action, schema_name, detail) "
                    "VALUES('schema_change', :fid, :detail)"
                ),
                {
                    "fid": form_id,
                    "detail": json.dumps(
                        {
                            "old_hash": existing[0],
                            "new_hash": merged_hash,
                            "columns": merged_cols,
                            "added_columns": new_cols,
                            "omitted_columns": missing_cols,
                        }
                    ),
                },
            )
            conn.execute(
                text(
                    "INSERT INTO qc_system.form_versions "
                    "(form_id, version_hash, column_manifest) "
                    "VALUES (:fid, :hash, :manifest)"
                ),
                {
                    "fid": form_id,
                    "hash": merged_hash,
                    "manifest": json.dumps(merged_cols),
                },
            )
            conn.commit()
            return True

        if not existing:
            logger.info(
                f"Registering baseline schema for '{form_id}' "
                f"(hash: {col_hash}, {len(incoming_cols)} columns)"
            )
            conn.execute(
                text(
                    "INSERT INTO qc_system.form_versions "
                    "(form_id, version_hash, column_manifest) "
                    "VALUES (:fid, :hash, :manifest)"
                ),
                {
                    "fid": form_id,
                    "hash": col_hash,
                    "manifest": json.dumps(incoming_cols),
                },
            )
            conn.commit()

    return True


@task(name="save-raw-bronze-layer")
def save_raw_to_minio(form_id: str, client: str, submissions: list):
    """Stores the immutable raw JSON payload in MinIO (raw-bronze bucket)."""
    logger = get_run_logger()
    if not submissions:
        return

    try:
        s3 = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_USER,
            aws_secret_access_key=MINIO_PASS,
            config=Config(signature_version='s3v4')
        )
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H%M%S')
        key       = f"{client}/{form_id}/{timestamp}/raw.json"
        body      = json.dumps(submissions, default=str).encode('utf-8')

        logger.info(f"Archiving {len(submissions)} submissions to raw-bronze/{key}")
        s3.put_object(Bucket='raw-bronze', Key=key, Body=body)
        logger.info(f"✓ MinIO archive written ({len(body):,} bytes)")

    except Exception as e:
        # MinIO failure is non-fatal — data is still written to postgres below.
        # Log the error but do not reraise so the upsert step proceeds.
        logger.warning(
            f"MinIO write failed for '{form_id}' — raw data will NOT be archived.\n"
            f"  Error: {e}\n"
            f"  PostgreSQL upsert will still proceed."
        )


@task(name="flatten-and-upsert-postgres", cache_policy=NO_CACHE)
def upsert_submissions(form_id: str, client_schema: str, submissions: list, engine):
    """
    Flattens SurveyCTO wide JSON, handles repeat groups, and upserts to PostgreSQL.

    Flat scalar fields    → client_schema.<form_id>  (primary key: submission_uuid)
    Repeat group arrays   → client_schema.<form_id>_<group_name>  (PK: _parent_uuid, _repeat_index)

    All column names are sanitized through _safe_id() to strip SQL-unsafe characters.
    The upsert uses ON CONFLICT DO UPDATE so re-running an ETL is always safe.
    """
    logger = get_run_logger()
    if not submissions:
        return

    safe_form_id       = _safe_id(form_id)
    safe_client_schema = _safe_id(client_schema)

    # ── Separate scalar fields from repeat groups ─────────────────────────────
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
                    if isinstance(item, dict):
                        item['_parent_uuid']  = sub['KEY']
                        item['_repeat_index'] = idx
                        repeat_groups[k].append(item)

    if not flat_rows:
        logger.warning(
            "No valid submissions after filtering (all missing 'KEY'). "
            "Aborting upsert."
        )
        return

    logger.info(
        f"Upserting {len(flat_rows)} submissions to {safe_client_schema}.{safe_form_id} "
        f"({len(repeat_groups)} repeat group(s))"
    )

    with engine.connect() as conn:
        df_base = pd.DataFrame(flat_rows)
        df_base.rename(columns={'KEY': 'submission_uuid'}, inplace=True)

        # Guarantee review_status column exists (older forms may not have it)
        if 'review_status' not in df_base.columns:
            df_base['review_status'] = 'unknown'

        target_table_name = safe_form_id
        target_table_fq   = f"{safe_client_schema}.{target_table_name}"
        stage_uuid        = uuid.uuid4().hex[:12]
        stage_table_name  = f"_stage_{stage_uuid}"

        logger.info(
            f"Staging {len(df_base)} rows in qc_system.{stage_table_name} "
            f"({len(df_base.columns)} columns)"
        )
        df_base.to_sql(
            stage_table_name, conn, schema='qc_system',
            if_exists='replace', index=False
        )

        try:
            columns_sql = ", ".join([_quote_ident(col) for col in df_base.columns])

            # Create target table on first sync
            table_exists = conn.execute(text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema=:s AND table_name=:t)"
            ), {"s": safe_client_schema, "t": target_table_name}).fetchone()[0]

            if not table_exists:
                logger.info(f"First sync: creating table '{target_table_fq}'")
                conn.execute(text(
                    f"CREATE TABLE {target_table_fq} AS "
                    f"SELECT * FROM qc_system.{stage_table_name} WITH NO DATA"
                ))
                conn.execute(text(
                    f"ALTER TABLE {target_table_fq} ADD PRIMARY KEY (submission_uuid)"
                ))
                conn.commit()
            else:
                existing_cols = set(conn.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=:s AND table_name=:t"
                ), {"s": safe_client_schema, "t": target_table_name}).scalars().all())
                missing_target_cols = [
                    col for col in df_base.columns
                    if col not in existing_cols
                ]
                for col in missing_target_cols:
                    logger.warning(
                        f"Adding new column '{col}' to '{target_table_fq}' from "
                        "SurveyCTO schema extension"
                    )
                    conn.execute(text(
                        f"ALTER TABLE {target_table_fq} "
                        f"ADD COLUMN {_quote_ident(col)} TEXT"
                    ))
                if missing_target_cols:
                    conn.commit()

            # Ensure updated_at column exists for change tracking
            if 'updated_at' not in df_base.columns:
                conn.execute(text(
                    f"ALTER TABLE {target_table_fq} "
                    f"ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()"
                ))
                conn.commit()

            # Build UPDATE SET — all columns except the primary key
            update_cols = [
                col for col in df_base.columns
                if col not in ('submission_uuid', 'KEY')
            ]
            update_set_sql = ", ".join(
                [
                    f"{_quote_ident(col)} = EXCLUDED.{_quote_ident(col)}"
                    for col in update_cols
                ]
            )
            if update_set_sql:
                update_set_sql += ', "updated_at" = NOW()'
            else:
                update_set_sql = '"updated_at" = NOW()'

            result = conn.execute(text(f"""
                INSERT INTO {target_table_fq} ({columns_sql})
                SELECT {columns_sql} FROM qc_system.{stage_table_name}
                ON CONFLICT (submission_uuid) DO UPDATE SET {update_set_sql}
            """))
            conn.commit()
            logger.info(f"✓ Upserted {len(df_base)} rows into '{target_table_fq}'")

        finally:
            conn.execute(text(f"DROP TABLE IF EXISTS qc_system.{stage_table_name}"))
            conn.commit()

        # ── Repeat groups → child tables ──────────────────────────────────────
        for group_name, rows in repeat_groups.items():
            safe_group       = _safe_id(group_name)
            child_table_name = f"{target_table_name}_{safe_group}"
            child_table_fq   = f"{safe_client_schema}.{child_table_name}"
            df_child         = pd.DataFrame(rows)
            stage_child_name = f"_stage_child_{stage_uuid}_{safe_group[:8]}"

            logger.info(
                f"Processing repeat group '{group_name}' "
                f"({len(df_child)} rows → {child_table_fq})"
            )
            df_child.to_sql(
                stage_child_name, conn, schema='qc_system',
                if_exists='replace', index=False
            )

            try:
                child_exists = conn.execute(text(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema=:s AND table_name=:t)"
                ), {"s": safe_client_schema, "t": child_table_name}).fetchone()[0]

                if not child_exists:
                    logger.info(f"First sync: creating child table '{child_table_fq}'")
                    conn.execute(text(
                        f"CREATE TABLE {child_table_fq} AS "
                        f"SELECT * FROM qc_system.{stage_child_name} WITH NO DATA"
                    ))
                    conn.execute(text(
                        f"ALTER TABLE {child_table_fq} "
                        f"ADD COLUMN IF NOT EXISTS _parent_uuid TEXT"
                    ))
                    conn.execute(text(
                        f"ALTER TABLE {child_table_fq} "
                        f"ADD COLUMN IF NOT EXISTS _repeat_index INT"
                    ))
                    conn.execute(text(
                        f"ALTER TABLE {child_table_fq} "
                        f"ADD PRIMARY KEY (_parent_uuid, _repeat_index)"
                    ))
                    conn.commit()

                existing_child_cols = set(conn.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=:s AND table_name=:t"
                ), {"s": safe_client_schema, "t": child_table_name}).scalars().all())
                missing_child_cols = [
                    col for col in df_child.columns
                    if col not in existing_child_cols
                ]
                for col in missing_child_cols:
                    logger.warning(
                        f"Adding new column '{col}' to '{child_table_fq}' from "
                        "SurveyCTO repeat-group schema extension"
                    )
                    conn.execute(text(
                        f"ALTER TABLE {child_table_fq} "
                        f"ADD COLUMN {_quote_ident(col)} TEXT"
                    ))
                if missing_child_cols:
                    conn.commit()

                child_columns = ", ".join([_quote_ident(col) for col in df_child.columns])
                conn.execute(text(f"""
                    INSERT INTO {child_table_fq} ({child_columns})
                    SELECT {child_columns} FROM qc_system.{stage_child_name}
                    ON CONFLICT (_parent_uuid, _repeat_index) DO NOTHING
                """))
                conn.commit()
                logger.info(f"✓ Upserted {len(df_child)} rows into '{child_table_fq}'")

            finally:
                conn.execute(text(
                    f"DROP TABLE IF EXISTS qc_system.{stage_child_name}"
                ))
                conn.commit()


@task(retries=1, name="execute-dbt-models")
def run_dbt_models():
    """
    Runs dbt build to refresh the analytical layer after every successful ingest.

    Failures are logged as warnings and do NOT reraise — the data upsert has
    already succeeded and the sync cursor has been advanced. Raising here would
    mark the Prefect flow as failed and risk confusing operators into thinking
    the ingestion itself failed.
    """
    logger  = get_run_logger()
    dbt_dir = os.path.join(PROJECT_ROOT, "dbt", "research_platform")
    logger.info(f"Running dbt build in {dbt_dir}")

    import subprocess
    try:
        result = subprocess.run(
            ["dbt", "build"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
    except Exception as exc:
        logger.warning(
            "dbt build could not be started; analytical layer may be stale.\n"
            f"  This is a warning, not an ingestion failure. Error: {exc}"
        )
        return

    if result.returncode != 0:
        logger.warning(
            "⚠️  dbt build returned non-zero exit code — analytical layer may be stale.\n"
            "  This is a warning, not a failure. The warehouse data is already committed.\n"
            + result.stdout[-2000:]
            + "\n"
            + result.stderr[-1000:]
        )
    else:
        logger.info("✅ dbt build complete.\n" + result.stdout[-2000:])


@flow(name="surveycto-ingestion-orchestrator", log_prints=True)
def run_etl(form_id: str, client: str, client_schema: str):
    """
    Master ETL workflow: SurveyCTO API → MinIO → PostgreSQL → dbt.

    Steps
    -----
    1. Read last_successful_sync cursor from qc_system.sync_state
    2. Fetch new/updated submissions from SurveyCTO API (incremental since cursor)
    3. Detect schema drift — halt if form columns have changed unexpectedly
    4. Archive raw JSON to MinIO raw-bronze bucket
    5. Flatten and upsert to the warehouse (client schema)
    6. Advance sync cursor to NOW()
    7. Run QC engine on the new data
    8. Run dbt build to refresh silver/gold models
    """
    logger = get_run_logger()
    logger.info(f"🚀 Starting ETL — form='{form_id}'  client='{client}'")

    engine    = create_engine(DB_URL)
    last_sync = None

    # Read incremental cursor
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT last_successful_sync FROM qc_system.sync_state "
                "WHERE pipeline_name = :n"
            ),
            {"n": form_id}
        ).fetchone()
        if row and row[0]:
            last_sync = row[0]
            logger.info(f"Resuming from cursor: {last_sync.isoformat()}")
        else:
            logger.info("No cursor found — performing full sync")

    submissions = None
    try:
        submissions = fetch_submissions(form_id, last_sync)

        if not submissions:
            logger.info("No new submissions since last sync. Nothing to do.")
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO qc_system.sync_state
                        (pipeline_name, last_run_status, updated_at)
                    VALUES (:n, 'success_no_data', NOW())
                    ON CONFLICT (pipeline_name) DO UPDATE SET
                        last_run_status='success_no_data', updated_at=NOW()
                """), {"n": form_id})
                conn.commit()
            return

        check_schema(form_id, submissions, engine)
        save_raw_to_minio(form_id, client, submissions)
        upsert_submissions(form_id, client_schema, submissions, engine)

        # Advance cursor AFTER successful upsert
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO qc_system.sync_state
                    (pipeline_name, last_successful_sync, last_run_status, updated_at)
                VALUES (:n, NOW(), 'success', NOW())
                ON CONFLICT (pipeline_name) DO UPDATE SET
                    last_successful_sync = NOW(),
                    last_run_status      = 'success',
                    updated_at           = NOW()
            """), {"n": form_id})
            conn.commit()

        logger.info(f"✨ Ingestion complete — {len(submissions)} submissions processed")

        # QC and dbt run immediately after ingest (also run on nightly schedule)
        from etl.qc.qc_engine import run_qc
        run_qc(form_id=form_id, client_schema=client_schema)
        run_dbt_models()

    except Exception as e:
        logger.error(f"❌ ETL pipeline failed for '{form_id}': {e}")
        try:
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO qc_system.failed_payloads
                        (form_id, client_schema, raw_payload, error_message)
                    VALUES (:f, :s, CAST(:p AS jsonb), :e)
                """), {
                    "f": form_id,
                    "s": client_schema,
                    "p": json.dumps(submissions, default=str) if submissions else "[]",
                    "e": str(e),
                })
                conn.execute(text("""
                    INSERT INTO qc_system.sync_state
                        (pipeline_name, last_run_status, updated_at)
                    VALUES (:n, 'failed', NOW())
                    ON CONFLICT (pipeline_name) DO UPDATE SET
                        last_run_status='failed', updated_at=NOW()
                """), {"n": form_id})
                conn.commit()
        except Exception as dlq_err:
            logger.error(f"Could not write failure state to DB: {dlq_err}")
        raise


if __name__ == "__main__":
    if len(sys.argv) > 3:
        # Direct invocation: python sync_surveycto.py <form_id> <client> <schema>
        run_etl(sys.argv[1], sys.argv[2], sys.argv[3])
    elif len(sys.argv) > 1:
        # Direct invocation: python sync_surveycto.py <form_id>
        form_id = sys.argv[1]
        config = get_form_config(form_id, active_only=False)
        if not config:
            raise SystemExit(
                f"Form '{form_id}' is not registered. Run: "
                f"python scripts/register_surveycto_form.py {form_id}"
            )
        run_etl(form_id, config["client"], config["schema"])
    else:
        deployments = []
        for form_id, config in load_form_registry(active_only=True).items():
            deployments.append(
                run_etl.to_deployment(
                    name=config["etl_deployment"],
                    cron=config["etl_cron"],
                    parameters={
                        "form_id":       form_id,
                        "client":        config["client"],
                        "client_schema": config["schema"],
                    },
                )
            )
        if not deployments:
            raise SystemExit("No active SurveyCTO forms registered.")
        serve(*deployments)
