#!/usr/bin/env python3
"""
Research Data Platform — Connectivity & Integrity Test Suite
Runs all pre-flight checks to verify the platform is working.
"""
import os
import sys
import json
import traceback

# ── Setup ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
ENV_PATH = os.path.join(PROJECT_DIR, "secrets", ".env")

from dotenv import load_dotenv
load_dotenv(ENV_PATH)

PASS = "✅ PASS"
FAIL = "❌ FAIL"
WARN = "⚠️  WARN"
results = []

def test(name, fn):
    try:
        ok, detail = fn()
        status = PASS if ok else FAIL
        results.append((status, name, detail))
        print(f"  {status}  {name}: {detail}")
    except Exception as e:
        results.append((FAIL, name, str(e)))
        print(f"  {FAIL}  {name}: {e}")

# ══════════════════════════════════════════
# 1. ENVIRONMENT VARIABLE CHECKS
# ══════════════════════════════════════════
print("\n" + "="*60)
print("SECTION 1: Environment Variables")
print("="*60)

def check_env_var(var_name):
    val = os.getenv(var_name)
    if val and not val.startswith("your_") and not val.startswith("YOUR") and not val.startswith("ACCOUNT"):
        return True, f"Set ({len(val)} chars)"
    elif val:
        return False, f"Placeholder value: '{val[:30]}...'"
    else:
        return False, "NOT SET"

for var in [
    "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB",
    "METABASE_DB_USER", "METABASE_DB_PASSWORD", "METABASE_DB_NAME",
    "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
    "WEBHOOK_SECRET",
]:
    test(f"ENV: {var}", lambda v=var: check_env_var(v))

# These are expected to be placeholders for now
for var in ["SURVEYCTO_SERVER_URL", "SURVEYCTO_USERNAME", "SURVEYCTO_PASSWORD"]:
    val = os.getenv(var)
    if val and (val.startswith("your_") or val.startswith("YOUR") or val.startswith("https://YOUR")):
        results.append((WARN, f"ENV: {var}", "Placeholder — needs real credentials before first run"))
        print(f"  {WARN}  ENV: {var}: Placeholder — needs real credentials before first run")
    elif val:
        results.append((PASS, f"ENV: {var}", f"Set ({len(val)} chars)"))
        print(f"  {PASS}  ENV: {var}: Set ({len(val)} chars)")
    else:
        results.append((FAIL, f"ENV: {var}", "NOT SET"))
        print(f"  {FAIL}  ENV: {var}: NOT SET")

# ══════════════════════════════════════════
# 2. DATABASE CONNECTIVITY
# ══════════════════════════════════════════
print("\n" + "="*60)
print("SECTION 2: PostgreSQL Database Connectivity")
print("="*60)

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")

engine = None
try:
    from sqlalchemy import create_engine, text
    DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@localhost:5435/{DB_NAME}"
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    test("DB: Connection to warehouse", lambda: (True, "Connected successfully"))
except Exception as e:
    test("DB: Connection to warehouse", lambda: (False, str(e)))

if engine:
    # Check schemas
    def check_schemas():
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name IN ('client_mtn','client_unilever','internal','qc_system')"
            )).fetchall()
            found = [r[0] for r in rows]
            expected = ['client_mtn', 'client_unilever', 'internal', 'qc_system']
            missing = [s for s in expected if s not in found]
            if missing:
                return False, f"Missing schemas: {missing}"
            return True, f"All 4 schemas present: {sorted(found)}"
    test("DB: Client schemas", check_schemas)

    # Check roles
    def check_roles():
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT rolname FROM pg_roles "
                "WHERE rolname IN ('etl_writer','analyst_reader','etl_svc','analyst','metabase_app')"
            )).fetchall()
            found = [r[0] for r in rows]
            expected = ['etl_writer', 'analyst_reader', 'etl_svc', 'analyst', 'metabase_app']
            missing = [r for r in expected if r not in found]
            if missing:
                return False, f"Missing roles: {missing}"
            return True, f"All 5 roles present"
    test("DB: Security roles", check_roles)

    # Check system tables
    def check_tables():
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='qc_system' ORDER BY table_name"
            )).fetchall()
            found = [r[0] for r in rows]
            expected = ['alert_suppression', 'audit_log', 'failed_payloads', 
                        'form_versions', 'pipeline_sla', 'qc_flags', 'sync_state']
            missing = [t for t in expected if t not in found]
            if missing:
                return False, f"Missing tables: {missing}"
            return True, f"All 7 system tables present"
    test("DB: System tables (qc_system)", check_tables)

    # Check SLA seed data
    def check_sla_data():
        with engine.connect() as conn:
            count = conn.execute(text("SELECT count(*) FROM qc_system.pipeline_sla")).fetchone()[0]
            return count == 4, f"{count}/4 SLA records seeded"
    test("DB: SLA seed data", check_sla_data)

    # Check metabaseappdb
    def check_metabase_db():
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT datname FROM pg_database WHERE datname='metabaseappdb'"
            )).fetchone()
            if row:
                return True, "metabaseappdb exists"
            return False, "metabaseappdb NOT found"
    test("DB: Metabase database", check_metabase_db)

    # Check constraints
    def check_qc_flags_constraints():
        with engine.connect() as conn:
            # Try inserting invalid severity — should fail
            try:
                conn.execute(text(
                    "INSERT INTO qc_system.qc_flags "
                    "(submission_uuid, client_schema, form_id, flag_type, severity, detail) "
                    "VALUES ('test','test','test','test','INVALID','{}'::jsonb)"
                ))
                conn.rollback()
                return False, "CHECK constraint on severity NOT enforced"
            except Exception:
                conn.rollback()
                return True, "CHECK constraint on severity is enforced"
    test("DB: qc_flags severity constraint", check_qc_flags_constraints)

# ══════════════════════════════════════════
# 3. MinIO OBJECT STORAGE
# ══════════════════════════════════════════
print("\n" + "="*60)
print("SECTION 3: MinIO Object Storage")
print("="*60)

try:
    import boto3
    from botocore.client import Config

    MINIO_USER = os.getenv("MINIO_ROOT_USER")
    MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD")

    s3 = boto3.client(
        's3',
        endpoint_url="http://localhost:9000",
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
        config=Config(signature_version='s3v4')
    )

    def check_minio_connection():
        buckets = s3.list_buckets()
        return True, f"Connected, {len(buckets['Buckets'])} buckets found"
    test("MinIO: Connection", check_minio_connection)

    # Check required buckets
    required_buckets = ['raw-bronze', 'processed-silver', 'exports', 'backup-staging']
    def check_minio_buckets():
        existing = [b['Name'] for b in s3.list_buckets()['Buckets']]
        missing = [b for b in required_buckets if b not in existing]
        if missing:
            return False, f"Missing buckets: {missing}"
        return True, f"All {len(required_buckets)} required buckets exist"
    test("MinIO: Required buckets", check_minio_buckets)

    # Create missing buckets if needed
    existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    missing_buckets = [b for b in required_buckets if b not in existing_buckets]
    if missing_buckets:
        print(f"\n  → Auto-creating missing buckets: {missing_buckets}")
        for bucket in missing_buckets:
            try:
                s3.create_bucket(Bucket=bucket)
                print(f"    Created bucket: {bucket}")
            except Exception as e:
                print(f"    Failed to create {bucket}: {e}")
        # Recheck
        test("MinIO: Buckets after auto-create", check_minio_buckets)

    # Test write/read cycle
    def check_minio_readwrite():
        test_key = "_test/connectivity_check.json"
        test_data = json.dumps({"test": True, "timestamp": "audit"}).encode('utf-8')
        s3.put_object(Bucket='raw-bronze', Key=test_key, Body=test_data)
        obj = s3.get_object(Bucket='raw-bronze', Key=test_key)
        read_back = obj['Body'].read()
        s3.delete_object(Bucket='raw-bronze', Key=test_key)
        if read_back == test_data:
            return True, "Write/Read/Delete cycle successful"
        return False, "Data mismatch on read-back"
    test("MinIO: Read/Write test", check_minio_readwrite)

except Exception as e:
    test("MinIO: Connection", lambda: (False, str(e)))

# ══════════════════════════════════════════
# 4. PREFECT ORCHESTRATION
# ══════════════════════════════════════════
print("\n" + "="*60)
print("SECTION 4: Prefect Server")
print("="*60)

try:
    import requests
    def check_prefect_health():
        r = requests.get("http://localhost:4200/api/health", timeout=5)
        if r.status_code == 200:
            return True, f"API healthy (HTTP {r.status_code})"
        return False, f"HTTP {r.status_code}"
    test("Prefect: API health", check_prefect_health)

    def check_prefect_version():
        r = requests.get("http://localhost:4200/api/admin/version", timeout=5)
        if r.status_code == 200:
            return True, f"Version: {r.text.strip()}"
        return False, f"HTTP {r.status_code}"
    test("Prefect: Server version", check_prefect_version)
except Exception as e:
    test("Prefect: API health", lambda: (False, str(e)))

# ══════════════════════════════════════════
# 5. PYTHON MODULE IMPORT CHECKS
# ══════════════════════════════════════════
print("\n" + "="*60)
print("SECTION 5: Python Module Imports")
print("="*60)

def check_import(module_name):
    try:
        __import__(module_name)
        return True, "Importable"
    except ImportError as e:
        return False, str(e)

for mod in ['pandas', 'sqlalchemy', 'prefect', 'boto3', 'requests', 
            'dotenv', 'fastapi', 'uvicorn']:
    test(f"Import: {mod}", lambda m=mod: check_import(m))

# ══════════════════════════════════════════
# 6. CODE-LEVEL STATIC ANALYSIS
# ══════════════════════════════════════════
print("\n" + "="*60)
print("SECTION 6: Code-Level Static Analysis")
print("="*60)

# Check webhook_server.py uses hmac.new vs hmac.new (should be hmac.new)
webhook_path = os.path.join(PROJECT_DIR, "webhook", "webhook_server.py")
with open(webhook_path, 'r') as f:
    webhook_code = f.read()

def check_hmac_bug():
    if "hmac.new(" in webhook_code:
        return False, "BUG: Uses hmac.new() — should be hmac.new() [Python has hmac.new, correct]"
    return True, "hmac usage is correct"

# Actually check: Python's hmac module has hmac.new() which IS correct
def check_hmac_usage():
    if "hmac.new(" in webhook_code:
        return True, "Uses hmac.new() correctly"
    elif "hmac.HMAC(" in webhook_code:
        return True, "Uses hmac.HMAC() correctly"
    return False, "No HMAC function call found"
test("Code: webhook HMAC usage", check_hmac_usage)

# Check password in DB_URL isn't URL-unsafe
def check_db_url_encoding():
    password = os.getenv("POSTGRES_PASSWORD", "")
    unsafe_chars = ['!', '@', '#', '$', '%', '&', '+', '=']
    found = [c for c in unsafe_chars if c in password]
    if found:
        return False, f"Password contains URL-unsafe chars {found} — needs urllib.parse.quote_plus()"
    return True, "Password is URL-safe"
test("Code: DB password URL-safety", check_db_url_encoding)

# Check backup script references minio-data directory
backup_path = os.path.join(PROJECT_DIR, "scripts", "backup_r2.sh")
with open(backup_path, 'r') as f:
    backup_code = f.read()

def check_backup_minio_dir():
    if "minio-data" in backup_code:
        # But we use Docker named volumes, so minio-data/ directory won't exist on host
        minio_dir = os.path.join(PROJECT_DIR, "minio-data")
        if not os.path.isdir(minio_dir):
            return False, "BUG: Script archives minio-data/ but we use Docker named volumes — directory won't exist"
        return True, "minio-data directory exists"
    return True, "No minio-data reference"
test("Code: backup_r2.sh minio-data path", check_backup_minio_dir)

# Check docker-compose.bi.yml Metabase networking
bi_path = os.path.join(PROJECT_DIR, "docker-compose.bi.yml")
with open(bi_path, 'r') as f:
    bi_code = f.read()

def check_bi_networking():
    issues = []
    if "extra_hosts" in bi_code and "127.0.0.1" in bi_code:
        issues.append("Metabase maps 'postgres' to 127.0.0.1 — but postgres is a separate container, not localhost")
    if "env_file" in bi_code and "${METABASE_DB_USER}" in bi_code:
        # Check if it uses variable interpolation which requires the host compose to load .env
        pass
    if issues:
        return False, "; ".join(issues)
    return True, "Networking looks correct"
test("Code: BI compose networking", check_bi_networking)

# Check docker-compose.yml hardcoded passwords in metabase-db-init
compose_path = os.path.join(PROJECT_DIR, "docker-compose.yml")
with open(compose_path, 'r') as f:
    compose_code = f.read()

def check_hardcoded_passwords():
    if "PlatformStr0ng!Pass2026" in compose_code:
        return False, "Hardcoded password in docker-compose.yml metabase-db-init entrypoint (should use env vars)"
    return True, "No hardcoded passwords"
test("Code: Hardcoded passwords in compose", check_hardcoded_passwords)

# Check __init__.py files exist for Python packages
def check_init_files():
    missing = []
    for d in ['etl', 'etl/pipelines', 'etl/qc', 'etl/utils', 'webhook']:
        init_path = os.path.join(PROJECT_DIR, d, '__init__.py')
        if not os.path.exists(init_path):
            missing.append(d)
    if missing:
        return False, f"Missing __init__.py in: {missing}"
    return True, "All Python packages have __init__.py"
test("Code: Python __init__.py files", check_init_files)

# Check sync_surveycto.py review_status handling
etl_path = os.path.join(PROJECT_DIR, "etl", "pipelines", "sync_surveycto.py")
with open(etl_path, 'r') as f:
    etl_code = f.read()

def check_review_status_bug():
    if "df_base.get('review_status'" in etl_code:
        return False, "BUG: df.get() on DataFrame returns a column or default — should use df['col'] with fallback"
    return True, "review_status handling is correct"
test("Code: ETL review_status handling", check_review_status_bug)

# Check qc_engine.py GPS ON CONFLICT DO NOTHING without unique constraint
qc_path = os.path.join(PROJECT_DIR, "etl", "qc", "qc_engine.py")
with open(qc_path, 'r') as f:
    qc_code = f.read()

def check_gps_on_conflict():
    if "ON CONFLICT DO NOTHING" in qc_code:
        return False, "BUG: ON CONFLICT DO NOTHING requires a unique constraint — qc_flags has no unique on (uuid, flag_type)"
    return True, "No bare ON CONFLICT"
test("Code: QC GPS ON CONFLICT usage", check_gps_on_conflict)

# ══════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════
print("\n" + "="*60)
print("AUDIT SUMMARY")
print("="*60)

passes = sum(1 for r in results if r[0] == PASS)
fails = sum(1 for r in results if r[0] == FAIL)
warns = sum(1 for r in results if r[0] == WARN)

print(f"\n  Total checks: {len(results)}")
print(f"  {PASS}: {passes}")
print(f"  {FAIL}: {fails}")
print(f"  {WARN}: {warns}")

if fails > 0:
    print(f"\n  FAILURES requiring fixes:")
    for status, name, detail in results:
        if status == FAIL:
            print(f"    → {name}: {detail}")

print("\n" + "="*60)
sys.exit(1 if fails > 0 else 0)
