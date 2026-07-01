#!/usr/bin/env python3
"""
Research Data Platform — Connectivity & Integrity Test Suite
Runs all pre-flight checks to verify the platform is working.
"""
import os
import sys
import json
import traceback

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# ── Setup ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
ENV_PATH = os.path.join(PROJECT_DIR, "secrets", ".env")

# Must add project root so etl.utils is importable before any other project imports
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

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
    "ETL_SVC_PASSWORD", "ANALYST_PASSWORD", "METABASE_APP_PASSWORD",
    "METABASE_DB_USER", "METABASE_DB_PASSWORD", "METABASE_DB_NAME",
    "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
    "WEBHOOK_SECRET", "SUPERSET_ADMIN_PASSWORD",
]:
    test(f"ENV: {var}", lambda v=var: check_env_var(v))

# SurveyCTO credentials — warn if still placeholder
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

# FIX: was building DB_URL as `f"postgresql://{user}:{password}@..."` which breaks
# silently when POSTGRES_PASSWORD contains URL-special chars (@, #, $, /, etc.).
# Now delegates to etl.utils.build_db_url() which applies urllib.parse.quote_plus.
try:
    from etl.utils import build_db_url
    DB_URL = build_db_url()
except ImportError as e:
    print(f"  {FAIL}  etl.utils import: {e}")
    DB_URL = None

engine = None
if DB_URL:
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        test("DB: Connection to warehouse", lambda: (True, "Connected successfully"))
    except Exception as e:
        test("DB: Connection to warehouse", lambda err=e: (False, str(err)))

if engine:
    # Check schemas
    def check_schemas():
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name IN ('client_mtn','client_unilever','internal','qc_system','bronze','silver','gold')"
            )).fetchall()
            found = [r[0] for r in rows]
            expected = ['client_mtn', 'client_unilever', 'internal', 'qc_system', 'bronze', 'silver', 'gold']
            missing = [s for s in expected if s not in found]
            if missing:
                return False, f"Missing schemas: {missing}"
            return True, f"All 7 schemas present"
    test("DB: Client + medallion schemas", check_schemas)

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
            return True, "All 5 roles present"
    test("DB: Security roles", check_roles)

    # Check system tables — FIX: was 9; respondent_locations added, now 10 expected
    def check_tables():
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='qc_system' ORDER BY table_name"
            )).fetchall()
            found = [r[0] for r in rows]
            expected = [
                'alert_suppression', 'audit_log', 'enumerator_scores',
                'failed_payloads', 'form_versions', 'gps_boundaries',
                'pipeline_sla', 'qc_flags', 'respondent_locations', 'sync_state'
            ]
            missing = [t for t in expected if t not in found]
            if missing:
                return False, f"Missing tables: {missing}"
            return True, f"All 10 system tables present"
    test("DB: System tables (qc_system)", check_tables)

    # Check haversine function exists (needed by GPS Phase 2)
    def check_haversine():
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT proname FROM pg_proc p "
                "JOIN pg_namespace n ON n.oid = p.pronamespace "
                "WHERE n.nspname = 'qc_system' AND p.proname = 'haversine_metres'"
            )).fetchone()
            if row:
                return True, "qc_system.haversine_metres() exists"
            return False, "qc_system.haversine_metres() NOT found"
    test("DB: haversine_metres function", check_haversine)

    # Check SLA table is ready. Per-form rows are synced from config/surveycto_forms.json.
    def check_sla_data():
        with engine.connect() as conn:
            count = conn.execute(text("SELECT count(*) FROM qc_system.pipeline_sla")).fetchone()[0]
            return True, f"{count} form SLA record(s) configured"
    test("DB: SLA config table", check_sla_data)

    # Check GPS boundaries table is ready. Real boundaries should be loaded per project/wave.
    def check_gps_boundaries():
        with engine.connect() as conn:
            count = conn.execute(text("SELECT count(*) FROM qc_system.gps_boundaries")).fetchone()[0]
            return True, f"{count} GPS boundary record(s) configured"
    test("DB: GPS boundaries table", check_gps_boundaries)

    # Check metabaseappdb
    def check_metabase_db():
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT datname FROM pg_database WHERE datname='metabaseappdb'"
            )).fetchone()
            if row:
                return True, "metabaseappdb exists"
            return False, "metabaseappdb NOT found (run: make up)"
    test("DB: Metabase database", check_metabase_db)

    # Check qc_flags CHECK constraint
    def check_qc_flags_constraints():
        with engine.connect() as conn:
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

    # Check service account passwords are not the placeholder
    def check_no_placeholder_passwords():
        with engine.connect() as conn:
            # Try to connect as etl_svc with the pending placeholder — should fail
            placeholder = "PENDING_SET_BY_INIT_SCRIPT"
            from sqlalchemy import create_engine as _ce
            from etl.utils import build_db_url
            etl_url = build_db_url().replace(
                f"{os.getenv('POSTGRES_USER')}:",
                "etl_svc:"
            ).replace(
                f":{os.getenv('POSTGRES_PASSWORD', '')}@",
                f":{placeholder}@"
            )
            try:
                test_engine = _ce(etl_url)
                with test_engine.connect() as c:
                    c.execute(text("SELECT 1"))
                # If we got here, placeholder password is accepted — bad
                return False, "etl_svc still has placeholder password — run `make up` again"
            except Exception:
                return True, "etl_svc placeholder password rejected (real password active)"
    test("DB: Service account passwords set", check_no_placeholder_passwords)

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

    required_buckets = ['raw-bronze', 'processed-silver', 'exports', 'backup-staging']

    def check_minio_buckets():
        existing = [b['Name'] for b in s3.list_buckets()['Buckets']]
        missing = [b for b in required_buckets if b not in existing]
        if missing:
            return False, f"Missing buckets: {missing} — run: make setup-buckets"
        return True, f"All {len(required_buckets)} required buckets exist"
    test("MinIO: Required buckets", check_minio_buckets)

    # Auto-create missing buckets so this test is also a light setup helper
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
        test("MinIO: Buckets after auto-create", check_minio_buckets)

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
    test("MinIO: Connection", lambda err=e: (False, str(err)))

# ══════════════════════════════════════════
# 4. PREFECT ORCHESTRATION
# ══════════════════════════════════════════
print("\n" + "="*60)
print("SECTION 4: Prefect Server")
print("="*60)

try:
    import requests
    def check_prefect_health():
        r = requests.get("http://localhost:4200/api/health", timeout=15)
        if r.status_code == 200:
            return True, f"API healthy (HTTP {r.status_code})"
        return False, f"HTTP {r.status_code}"
    test("Prefect: API health", check_prefect_health)

    def check_prefect_version():
        r = requests.get("http://localhost:4200/api/admin/version", timeout=15)
        if r.status_code == 200:
            return True, f"Version: {r.text.strip()}"
        return False, f"HTTP {r.status_code}"
    test("Prefect: Server version", check_prefect_version)
except Exception as e:
    test("Prefect: API health", lambda err=e: (False, str(err)))

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

# Check dbt is installed and deps are satisfied
def check_dbt():
    import subprocess
    r = subprocess.run(["dbt", "--version"], capture_output=True, text=True)
    if r.returncode == 0:
        version_line = r.stdout.strip().splitlines()[0] if r.stdout else "unknown"
        return True, version_line
    return False, f"dbt not found — run: make install"
test("Import: dbt CLI", check_dbt)

def check_dbt_deps():
    import subprocess
    dbt_dir = os.path.join(PROJECT_DIR, "dbt", "research_platform")
    packages_lock = os.path.join(dbt_dir, "package-lock.yml")
    dbt_packages_dir = os.path.join(dbt_dir, "dbt_packages")
    if not os.path.isdir(dbt_packages_dir) or not os.listdir(dbt_packages_dir):
        return False, "dbt_packages/ is empty — run: make install  (calls dbt deps)"
    return True, "dbt_packages/ populated"
test("dbt: packages installed", check_dbt_deps)

# ══════════════════════════════════════════
# 6. CODE-LEVEL STATIC ANALYSIS
# ══════════════════════════════════════════
print("\n" + "="*60)
print("SECTION 6: Code-Level Static Analysis")
print("="*60)

webhook_path = os.path.join(PROJECT_DIR, "webhook", "webhook_server.py")
with open(webhook_path, 'r', encoding='utf-8') as f:
    webhook_code = f.read()

def check_hmac_usage():
    if "hmac.new(" in webhook_code:
        return True, "Uses hmac.new() correctly"
    elif "hmac.HMAC(" in webhook_code:
        return True, "Uses hmac.HMAC() correctly"
    return False, "No HMAC function call found"
test("Code: webhook HMAC usage", check_hmac_usage)

def check_db_url_encoding():
    password = os.getenv("POSTGRES_PASSWORD", "")
    unsafe_chars = ['!', '@', '#', '$', '%', '&', '+', '=']
    found = [c for c in unsafe_chars if c in password]
    if found:
        from etl.utils import build_db_url
        if password not in build_db_url():
            return True, f"Password contains URL-unsafe chars {found}, encoded by build_db_url()"
        return False, f"Password contains URL-unsafe chars {found} and is not encoded"
    return True, "Password is URL-safe"
test("Code: DB URL encoding", check_db_url_encoding)

def check_webhook_uses_build_db_url():
    if "build_db_url" in webhook_code:
        return True, "webhook_server.py uses build_db_url()"
    return False, "webhook_server.py still uses raw f-string for DB_URL"
test("Code: webhook_server uses build_db_url", check_webhook_uses_build_db_url)

backup_path = os.path.join(PROJECT_DIR, "scripts", "backup_r2.sh")
with open(backup_path, 'r', encoding='utf-8') as f:
    backup_code = f.read()

def check_backup_minio_volume():
    if "docker run" in backup_code and "rp-minio-data" in backup_code:
        return True, "backup_r2.sh correctly exports via Docker named volume"
    if "minio-data/" in backup_code and "docker run" not in backup_code:
        return False, "backup_r2.sh archives a bind-mount path — should export via Docker named volume"
    return True, "MinIO backup approach looks correct"
test("Code: backup_r2.sh minio volume export", check_backup_minio_volume)

bi_path = os.path.join(PROJECT_DIR, "docker-compose.bi.yml")
with open(bi_path, 'r', encoding='utf-8') as f:
    bi_code = f.read()

def check_bi_networking():
    issues = []
    if "extra_hosts" in bi_code and "127.0.0.1" in bi_code:
        issues.append("Metabase maps 'postgres' to 127.0.0.1 — postgres is a Docker container, not localhost")
    if issues:
        return False, "; ".join(issues)
    return True, "BI compose networking looks correct"
test("Code: BI compose networking", check_bi_networking)

compose_path = os.path.join(PROJECT_DIR, "docker-compose.yml")
with open(compose_path, 'r', encoding='utf-8') as f:
    compose_code = f.read()

def check_hardcoded_passwords():
    if "PlatformStr0ng!Pass2026" in compose_code:
        return False, "Hardcoded password in docker-compose.yml (should use env vars)"
    return True, "No hardcoded passwords in docker-compose.yml"
test("Code: Hardcoded passwords in compose", check_hardcoded_passwords)

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

etl_path = os.path.join(PROJECT_DIR, "etl", "pipelines", "sync_surveycto.py")
with open(etl_path, 'r', encoding='utf-8') as f:
    etl_code = f.read()

def check_active_forms_served():
    if "load_form_registry" in etl_code and "serve(*deployments)" in etl_code:
        return True, "ETL deployments are generated from config/surveycto_forms.json"
    return False, "ETL deployments should be generated from the SurveyCTO form registry"
test("Code: Registry-driven ETL form deployments", check_active_forms_served)

def check_dbt_not_fatal():
    function_body = etl_code.split("def run_dbt_models", 1)[1].split("@flow", 1)[0]
    if "logger.warning" in function_body and "raise" not in function_body.replace("reraise", ""):
        return True, "dbt failures are non-fatal (logged as warning)"
    return False, "dbt failure still raises — will mark ETL flow as failed"
test("Code: dbt failure is non-fatal", check_dbt_not_fatal)

qc_path = os.path.join(PROJECT_DIR, "etl", "qc", "qc_engine.py")
with open(qc_path, 'r', encoding='utf-8') as f:
    qc_code = f.read()

def check_gps_on_conflict():
    if "ON CONFLICT DO NOTHING" in qc_code:
        return False, "BUG: ON CONFLICT DO NOTHING without unique constraint — use WHERE NOT EXISTS"
    return True, "No bare ON CONFLICT DO NOTHING"
test("Code: QC GPS ON CONFLICT usage", check_gps_on_conflict)

def check_compute_scores_not_stub():
    if "Enumerator metric scoreboard triggers registered" in qc_code:
        return False, "compute_scores() is still a stub — replace with real scoring SQL"
    if "enumerator_scores" in qc_code and "quality_score" in qc_code:
        return True, "compute_scores() writes real scores to enumerator_scores table"
    return False, "compute_scores() may not be fully implemented"
test("Code: compute_scores() is not a stub", check_compute_scores_not_stub)

# Check regional_performance.sql doesn't reference duration_seconds unsafely
rp_path = os.path.join(PROJECT_DIR, "dbt", "research_platform", "models", "gold", "regional_performance.sql")
with open(rp_path, 'r', encoding='utf-8') as f:
    rp_code = f.read()

def check_regional_duration():
    # duration_seconds is a form field that may not exist; the safe pattern
    # is to derive duration from starttime/endtime (always present in SurveyCTO)
    if "duration_seconds" in rp_code and "starttime" not in rp_code:
        return False, (
            "regional_performance.sql references duration_seconds directly — "
            "this column may not exist in all forms. Use NULLIF(starttime,'')::timestamptz approach."
        )
    return True, "Duration column handling is safe"
test("Code: regional_performance duration column", check_regional_duration)

# Check gold models exist for all 3 clients
gold_dir = os.path.join(PROJECT_DIR, "dbt", "research_platform", "models", "gold")
def check_gold_models_coverage():
    gold_files = os.listdir(gold_dir)
    # Check by exact expected filename so the test is unambiguous
    required = ["fct_mtn_survey_responses.sql", "fct_unilever_retail.sql", "fct_internal_census.sql"]
    missing  = [f for f in required if f not in gold_files]
    if missing:
        return False, f"Missing gold fact models: {missing}"
    return True, "Gold fact models present for all 3 clients (mtn, unilever, internal)"
test("Code: Gold models for all 3 clients", check_gold_models_coverage)

# ══════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════
print("\n" + "="*60)
print("AUDIT SUMMARY")
print("="*60)

passes = sum(1 for r in results if r[0] == PASS)
fails  = sum(1 for r in results if r[0] == FAIL)
warns  = sum(1 for r in results if r[0] == WARN)

print(f"\n  Total checks: {len(results)}")
print(f"  {PASS}: {passes}")
print(f"  {FAIL}: {fails}")
print(f"  {WARN}: {warns}")

if fails > 0:
    print(f"\n  Failures requiring fixes:")
    for status, name, detail in results:
        if status == FAIL:
            print(f"    → {name}: {detail}")

print("\n" + "="*60)
sys.exit(1 if fails > 0 else 0)
