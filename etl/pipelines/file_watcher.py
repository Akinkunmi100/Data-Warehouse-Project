import os, sys, json, time, uuid, shutil, threading
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

ENV_PATH = os.path.join(PROJECT_ROOT, "secrets", ".env")
load_dotenv(ENV_PATH)

import pandas as pd
import boto3
from botocore.client import Config
from sqlalchemy import create_engine, text
from prefect import flow, task
from prefect.logging import get_run_logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from etl.utils import safe_id, build_db_url

# FIX: was building DB_URL with an inline f-string + manual quote_plus.
# Now uses the shared build_db_url() from etl.utils — single source of truth.
DB_URL = build_db_url()
MINIO_USER     = os.getenv("MINIO_ROOT_USER")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = "http://localhost:9000"

UPLOADS_DIR   = Path(PROJECT_ROOT) / "uploads"
PROCESSED_DIR = UPLOADS_DIR / "_processed"
FAILED_DIR    = UPLOADS_DIR / "_failed"
SUPPORTED     = {".csv", ".tsv", ".xlsx", ".xls", ".sav"}

STABILISE_SECS    = 2.0
STABILISE_RETRIES = 10


def _wait_for_stable(path: Path) -> bool:
    """Block until the file size stops changing (write complete). Returns False on timeout."""
    prev = -1
    for _ in range(STABILISE_RETRIES):
        try:
            cur = path.stat().st_size
        except FileNotFoundError:
            return False
        if cur == prev and cur > 0:
            return True
        prev = cur
        time.sleep(STABILISE_SECS)
    return False


def _read_file(path: Path) -> "pd.DataFrame":
    ext = path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    if ext == ".tsv":
        return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str, keep_default_na=False)
    if ext == ".sav":
        import pyreadstat
        df, _ = pyreadstat.read_sav(str(path))
        return df.astype(str)
    raise ValueError(f"Unsupported extension: {ext}")


@task(name="validate-uploaded-file")
def validate_file(path: Path) -> dict:
    logger = get_run_logger()
    if path.suffix.lower() not in SUPPORTED:
        raise ValueError(f"Unsupported format '{path.suffix}'. Accepted: {SUPPORTED}")
    df = _read_file(path)
    if df.empty:
        raise ValueError(f"File '{path.name}' is empty.")
    if len(df.columns) < 2:
        raise ValueError(f"File has only {len(df.columns)} column(s) — likely malformed.")
    df.columns = [safe_id(str(c).strip()) for c in df.columns]
    logger.info(f"Validated '{path.name}': {len(df)} rows x {len(df.columns)} columns")
    return {"dataframe": df, "rows": len(df), "columns": list(df.columns)}


@task(name="save-raw-file-to-minio")
def save_raw(path: Path, meta: dict) -> str:
    logger = get_run_logger()
    s3 = boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
        config=Config(signature_version="s3v4"),
    )
    ts  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%S")
    key = f"internal/uploads/{ts}_{path.name}"
    with open(path, "rb") as fh:
        s3.put_object(Bucket="raw-bronze", Key=key, Body=fh.read())
    logger.info(f"Archived to raw-bronze/{key}")
    return key


@task(name="load-file-to-postgres")
def load_to_postgres(path: Path, meta: dict) -> int:
    logger = get_run_logger()
    df         = meta["dataframe"]
    engine     = create_engine(DB_URL)
    table_name = safe_id(path.stem.lower())
    stage_name = f"_stage_{uuid.uuid4().hex[:10]}"

    with engine.connect() as conn:
        df.to_sql(stage_name, conn, schema="qc_system", if_exists="replace", index=False)

        exists = conn.execute(text(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='internal' AND table_name=:t)"
        ), {"t": table_name}).fetchone()[0]

        cols_sql = ", ".join([f'"{c}"' for c in df.columns])

        if not exists:
            conn.execute(text(
                f"CREATE TABLE internal.{table_name} AS "
                f"SELECT * FROM qc_system.{stage_name} WITH NO DATA"
            ))
            conn.execute(text(
                f"ALTER TABLE internal.{table_name} "
                f"ADD COLUMN IF NOT EXISTS _file_row_id BIGSERIAL PRIMARY KEY"
            ))
            conn.commit()
            logger.info(f"Created internal.{table_name}")

        conn.execute(text(
            f"INSERT INTO internal.{table_name} ({cols_sql}) "
            f"SELECT {cols_sql} FROM qc_system.{stage_name}"
        ))
        conn.execute(text(f"DROP TABLE IF EXISTS qc_system.{stage_name}"))
        conn.commit()

    logger.info(f"Loaded {len(df)} rows into internal.{table_name}")
    return len(df)


@task(name="write-file-audit-log")
def write_audit(path: Path, minio_key: str, row_count: int, status: str, error: str = ""):
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(text(
            "INSERT INTO qc_system.audit_log (action, schema_name, detail) "
            "VALUES ('file_ingest', 'internal', :d)"
        ), {"d": json.dumps({
            "filename":    path.name,
            "minio_key":   minio_key,
            "row_count":   row_count,
            "status":      status,
            "error":       error,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        })})
        conn.commit()


@flow(name="file-ingest", log_prints=True)
def ingest_file(filepath: str):
    """Full ingest flow for one uploaded file: validate -> archive -> load -> audit."""
    logger    = get_run_logger()
    path      = Path(filepath)
    minio_key = ""
    row_count = 0
    logger.info(f"Processing upload: {path.name}")

    try:
        meta      = validate_file(path)
        minio_key = save_raw(path, meta)
        row_count = load_to_postgres(path, meta)

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        ts   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        dest = PROCESSED_DIR / f"{ts}_{path.name}"
        shutil.move(str(path), str(dest))

        write_audit(path, minio_key, row_count, "success")
        logger.info(f"Done: {path.name} ({row_count} rows)")

    except Exception as exc:
        logger.error(f"Failed: {path.name} — {exc}")
        FAILED_DIR.mkdir(parents=True, exist_ok=True)
        ts   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        dest = FAILED_DIR / f"{ts}_{path.name}"
        try:
            shutil.move(str(path), str(dest))
        except Exception:
            pass
        write_audit(path, minio_key, row_count, "failed", str(exc))
        raise


class UploadHandler(FileSystemEventHandler):
    """Watchdog handler: fires ingest_file() in a daemon thread on each new file."""

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() not in SUPPORTED:
            return
        if path.parent.name.startswith("_"):
            return  # ignore _processed/ and _failed/

        print(f"[watchdog] Detected: {path.name} — waiting for write completion...")
        if not _wait_for_stable(path):
            print(f"[watchdog] Timed out on {path.name} — skipping.")
            return

        threading.Thread(
            target=lambda: _safe_run(str(path)), daemon=True
        ).start()


def _safe_run(filepath: str):
    try:
        ingest_file(filepath)
    except Exception as exc:
        print(f"[watchdog] Flow error for {filepath}: {exc}")


if __name__ == "__main__":
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    observer = Observer()
    observer.schedule(UploadHandler(), str(UPLOADS_DIR), recursive=False)
    observer.start()
    print(f"File watchdog active — monitoring: {UPLOADS_DIR}")
    print(f"Formats: {', '.join(sorted(SUPPORTED))} | Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
