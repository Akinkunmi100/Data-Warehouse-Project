# ══════════════════════════════════════════
# Research Data Platform — FastAPI Webhook Receiver
# ══════════════════════════════════════════

import os
import json
import uuid
import hmac
import hashlib
from typing import Dict, Any
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Header, status
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import boto3
from botocore.client import Config
from sqlalchemy import create_engine, text
from datetime import datetime, timezone

# Dynamic environmental loading relative to script path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, "..", "secrets", ".env")
load_dotenv(ENV_PATH)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
if not WEBHOOK_SECRET:
    raise RuntimeError(
        "WEBHOOK_SECRET is not set in secrets/.env — refusing to start an "
        "unauthenticated webhook receiver. Generate one with: "
        "python -c \"import secrets; print(secrets.token_hex(32))\""
    )
MINIO_USER     = os.getenv("MINIO_ROOT_USER")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = "http://localhost:9000"

# FIX: was building DB_URL without URL-encoding the password.
# Uses etl.utils.build_db_url() which applies urllib.parse.quote_plus.
import sys as _sys
import os as _os
_WEBHOOK_DIR = _os.path.dirname(_os.path.abspath(__file__))
_PROJECT_ROOT = _os.path.abspath(_os.path.join(_WEBHOOK_DIR, ".."))
if _PROJECT_ROOT not in _sys.path:
    _sys.path.insert(0, _PROJECT_ROOT)
from etl.utils import build_db_url
DB_URL = build_db_url()

app    = FastAPI(title="Research Data Platform Webhook Gateway")
engine = create_engine(DB_URL)

FORM_SCHEMA_MAP = {
    "project_appraise": {"client": "client_mtn",      "schema": "client_mtn"},
    "unilever-retail":  {"client": "client_unilever",  "schema": "client_unilever"},
    "internal-census":  {"client": "internal",         "schema": "internal"},
}

# FIX: map each schema to its registered Prefect deployment name so that
# webhook-triggered runs appear in the Prefect UI like any scheduled run.
DEPLOYMENT_MAP = {
    "client_mtn":      "surveycto-ingestion-orchestrator/surveycto-nightly-mtn",
    "client_unilever": "surveycto-ingestion-orchestrator/surveycto-nightly-unilever",
    "internal":        "surveycto-ingestion-orchestrator/surveycto-nightly-internal",
}

# FIX Bug 4: Prefect 3.x moved run_deployment out of prefect.deployments.
# Import at module level with a try/except chain so the failure surfaces once
# at startup (with a clear message) rather than on every webhook request.
# If neither import path works, _run_deployment is set to None and the webhook
# falls back to the subprocess path with an explanatory warning.
try:
    from prefect.deployments import run_deployment as _run_deployment
except ImportError:
    try:
        from prefect.deployments.deployments import run_deployment as _run_deployment
    except ImportError:
        import warnings
        warnings.warn(
            "Could not import run_deployment from Prefect. "
            "Webhook-triggered runs will fall back to subprocess (not visible in Prefect UI).",
            RuntimeWarning,
            stacklevel=1,
        )
        _run_deployment = None


def verify_signature(payload: bytes, signature: str) -> bool:
    """Verifies HMAC signature matching from sender."""
    if not signature:
        return False
    expected = hmac.new(WEBHOOK_SECRET.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def write_to_dlq(form_id: str, client_schema: str, payload: Any, error_msg: str):
    """Saves bad payloads to the dead letter queue for admin review."""
    try:
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO qc_system.failed_payloads (form_id, client_schema, raw_payload, error_message, status)
                    VALUES (:form_id, :schema, CAST(:payload AS jsonb), :error_msg, 'pending')
                """),
                {
                    "form_id":   form_id,
                    "schema":    client_schema,
                    "payload":   json.dumps(payload),
                    "error_msg": error_msg
                }
            )
            conn.commit()
    except Exception as e:
        print(f"FAILED TO WRITE TO DLQ: {e}")


async def process_and_trigger(form_id: str, client: str, schema: str, payload: Dict[str, Any]):
    """Saves payload to MinIO then triggers a Prefect flow run via the Prefect API.

    FIX Bug 4: was using subprocess.Popen() which spawned a raw Python process completely
    outside Prefect — runs were invisible in the UI with no logging, retries, or
    cancellation. Now uses run_deployment() so every webhook-triggered run appears
    in the Prefect UI alongside scheduled runs.

    Falls back to subprocess if the Prefect API is unreachable (e.g. during startup)
    or if run_deployment could not be imported.
    """
    import re

    # Sanitize arguments before any subprocess fallback
    safe_form   = re.sub(r'[^a-zA-Z0-9_\-]', '', form_id)
    safe_client = re.sub(r'[^a-zA-Z0-9_\-]', '', client)
    safe_schema = re.sub(r'[^a-zA-Z0-9_\-]', '', schema)

    # ── 1. Store raw payload in MinIO ─────────────────────────────────────────
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_USER,
            aws_secret_access_key=MINIO_PASS,
            config=Config(signature_version='s3v4')
        )
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H%M%S')
        s3_key    = f"{client}/{form_id}/webhook_{timestamp}_{uuid.uuid4().hex[:8]}.json"
        s3.put_object(
            Bucket='raw-bronze',
            Key=s3_key,
            Body=json.dumps(payload).encode('utf-8')
        )
    except Exception as e:
        print(f"MinIO write failed for {form_id}: {e}")
        write_to_dlq(form_id, schema, payload, f"MinIO write error: {e}")
        return

    # ── 2. Trigger Prefect flow run via API (visible in UI) ───────────────────
    deployment_name = DEPLOYMENT_MAP.get(schema)
    if not deployment_name:
        print(f"No deployment mapping for schema '{schema}' — cannot trigger Prefect run.")
        write_to_dlq(form_id, schema, payload, f"No deployment mapping for schema: {schema}")
        return

    # FIX Bug 4: use the module-level _run_deployment (resolved once at startup).
    if _run_deployment is not None:
        try:
            await _run_deployment(
                name=deployment_name,
                parameters={
                    "form_id":       safe_form,
                    "client":        safe_client,
                    "client_schema": safe_schema,
                },
                timeout=0,  # fire-and-forget; don't block the webhook response
            )
            print(f"✅ Prefect run_deployment triggered: {deployment_name}")
            return
        except Exception as prefect_err:
            # Prefect API unreachable — fall through to subprocess fallback below.
            print(f"⚠️  Prefect API unreachable ({prefect_err}) — falling back to subprocess for {form_id}.")
    else:
        print(f"⚠️  run_deployment unavailable — falling back to subprocess for {form_id}.")

    # ── 3. Subprocess fallback (run not visible in Prefect UI) ────────────────
    try:
        import subprocess
        root_dir        = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
        etl_script_path = os.path.join(root_dir, "etl", "pipelines", "sync_surveycto.py")
        subprocess.Popen(["python3", etl_script_path, safe_form, safe_client, safe_schema])
    except Exception as sub_err:
        print(f"Subprocess fallback also failed: {sub_err}")
        write_to_dlq(form_id, schema, payload, f"Both Prefect and subprocess trigger failed: {sub_err}")


@app.post("/webhook/v1/{form_id}", status_code=status.HTTP_202_ACCEPTED)
async def receive_survey(
    form_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    x_surveycto_signature: str = Header(None)
):
    """High-performance webhook receiver for SurveyCTO forms."""
    body = await request.body()

    if x_surveycto_signature:
        if not verify_signature(body, x_surveycto_signature):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid cryptographic signature header."
            )
    else:
        token = request.query_params.get("token")
        if token != WEBHOOK_SECRET:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authorization token missing or invalid."
            )

    mapping = FORM_SCHEMA_MAP.get(form_id)
    if not mapping:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Form ID '{form_id}' is not mapped to any client schema."
        )

    try:
        payload = json.loads(body)
    except Exception:
        write_to_dlq(form_id, mapping['schema'], {"raw_body": body.decode('utf-8', errors='ignore')}, "Malformed JSON body")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Payload is not valid JSON."
        )

    if isinstance(payload, list):
        if not payload or 'KEY' not in payload[0]:
            write_to_dlq(form_id, mapping['schema'], payload, "Invalid SurveyCTO wide schema format: KEY is missing.")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid SurveyCTO structure: missing KEY."
            )
    elif isinstance(payload, dict):
        if 'KEY' not in payload:
            write_to_dlq(form_id, mapping['schema'], payload, "Invalid SurveyCTO wide schema format: KEY is missing.")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid SurveyCTO structure: missing KEY."
            )
        payload = [payload]

    background_tasks.add_task(
        process_and_trigger,
        form_id=form_id,
        client=mapping['client'],
        schema=mapping['schema'],
        payload=payload
    )

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"status": "accepted", "message": "Payload queued for ingest.", "form_id": form_id}
    )


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "webhook_gateway"}
