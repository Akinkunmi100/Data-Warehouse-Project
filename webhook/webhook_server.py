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

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "default_secret")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = "http://localhost:9000"

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@localhost:5435/{DB_NAME}"

app = FastAPI(title="Research Data Platform Webhook Gateway")
engine = create_engine(DB_URL)

FORM_SCHEMA_MAP = {
    "project_appraise": {"client": "client_mtn", "schema": "client_mtn"},
    "unilever-retail": {"client": "client_unilever", "schema": "client_unilever"},
    "internal-census": {"client": "internal", "schema": "internal"}
}

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
                    "form_id": form_id,
                    "schema": client_schema,
                    "payload": json.dumps(payload),
                    "error_msg": error_msg
                }
            )
            conn.commit()
    except Exception as e:
        print(f"FAILED TO WRITE TO DLQ: {e}")

def process_and_trigger(form_id: str, client: str, schema: str, payload: Dict[str, Any]):
    """Asynchronous worker to save payload and trigger Prefect flow."""
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_USER,
            aws_secret_access_key=MINIO_PASS,
            config=Config(signature_version='s3v4')
        )
        
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H%M%S')
        s3_key = f"{client}/{form_id}/webhook_{timestamp}_{uuid.uuid4().hex[:8]}.json"
        
        s3.put_object(
            Bucket='raw-bronze',
            Key=s3_key,
            Body=json.dumps(payload).encode('utf-8')
        )
        
        import subprocess, re
        # Dynamically resolve ETL script absolute path
        root_dir = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
        etl_script_path = os.path.join(root_dir, "etl", "pipelines", "sync_surveycto.py")
        
        # Sanitize all arguments to prevent injection (allow only alphanumeric, hyphens, underscores)
        safe_form   = re.sub(r'[^a-zA-Z0-9_\-]', '', form_id)
        safe_client = re.sub(r'[^a-zA-Z0-9_\-]', '', client)
        safe_schema = re.sub(r'[^a-zA-Z0-9_\-]', '', schema)
        
        subprocess.Popen(["python3", etl_script_path, safe_form, safe_client, safe_schema])
        
    except Exception as e:
        print(f"Error executing webhook integration worker: {e}")
        write_to_dlq(form_id, schema, payload, str(e))

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
    except Exception as e:
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
