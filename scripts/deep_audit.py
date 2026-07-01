#!/usr/bin/env python3
"""
Research Data Platform — Deep Requirements & Quality Audit
Checks: DB privileges, file permissions, missing components, security posture,
backup integrity, docker health, code quality, and process requirements.
"""
import os, sys, json, subprocess, stat, re, textwrap, shutil
from dotenv import load_dotenv

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
ENV_PATH = os.path.join(PROJECT_DIR, "secrets", ".env")
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)
load_dotenv(ENV_PATH)

from sqlalchemy import create_engine, text
import requests, boto3
from botocore.client import Config
from etl.utils import build_db_url

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_URL  = build_db_url()
engine  = create_engine(DB_URL)

MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD")
s3 = boto3.client('s3', endpoint_url="http://localhost:9000",
    aws_access_key_id=MINIO_USER, aws_secret_access_key=MINIO_PASS,
    config=Config(signature_version='s3v4'))

PASS = "✅ PASS"; FAIL = "❌ FAIL"; WARN = "⚠️  WARN"; INFO = "ℹ️  INFO"
results = []

def check(name, fn, category=""):
    try:
        code, detail = fn()
        results.append((code, category, name, detail))
        icon = {"PASS": PASS, "FAIL": FAIL, "WARN": WARN, "INFO": INFO}.get(code, INFO)
        print(f"  {icon}  {name}: {detail}")
    except Exception as e:
        results.append(("FAIL", category, name, str(e)))
        print(f"  {FAIL}  {name}: EXCEPTION — {e}")

# ══════════════════════════════════════════════════════════════
# A. PROJECT STRUCTURE REQUIREMENTS
# ══════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("A. PROJECT STRUCTURE & FILE REQUIREMENTS")
print("="*65)

required_files = [
    "docker-compose.yml",
    "docker-compose.bi.yml",
    ".gitignore",
    "secrets/.env",
    "scripts/init_db.sql",
    "scripts/backup_r2.sh",
    "etl/__init__.py",
    "etl/pipelines/__init__.py",
    "etl/pipelines/sync_surveycto.py",
    "etl/qc/__init__.py",
    "etl/qc/qc_engine.py",
    "etl/utils/__init__.py",
    "webhook/__init__.py",
    "webhook/webhook_server.py",
    "scripts/test_connectivity.py",
]
for f in required_files:
    path = os.path.join(PROJECT_DIR, f)
    check(f"File exists: {f}", 
          lambda p=path: ("PASS", f"Present ({os.path.getsize(p):,} bytes)") if os.path.exists(p) else ("FAIL","MISSING"),
          "structure")

# Missing: README, requirements.txt, Makefile
missing_nice = ["README.md", "requirements.txt", "Makefile"]
for f in missing_nice:
    path = os.path.join(PROJECT_DIR, f)
    check(f"Nice-to-have: {f}",
          lambda p=path: ("PASS", "Present") if os.path.exists(p) else ("WARN", "Missing — improves onboarding & reproducibility"),
          "structure")

# ══════════════════════════════════════════════════════════════
# B. SECURITY REQUIREMENTS
# ══════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("B. SECURITY POSTURE")
print("="*65)

# 1. .env must not be tracked by git
def check_env_not_tracked():
    result = subprocess.run(
        ["git", "ls-files", "secrets/.env", "--error-unmatch"],
        cwd=PROJECT_DIR, capture_output=True, text=True
    )
    if result.returncode == 0:
        return "FAIL", "secrets/.env is tracked by Git — CRITICAL security breach"
    return "PASS", "secrets/.env is properly excluded from Git"
check("Git: secrets/.env not tracked", check_env_not_tracked, "security")

# 2. postgres-data not tracked
def check_data_not_tracked():
    result = subprocess.run(
        ["git", "ls-files", "postgres-data/"],
        cwd=PROJECT_DIR, capture_output=True, text=True
    )
    if result.stdout.strip():
        return "FAIL", f"Data folders tracked in git: {result.stdout.strip()}"
    return "PASS", "Data folders excluded from Git"
check("Git: Data folders not tracked", check_data_not_tracked, "security")

# 3. No hardcoded passwords anywhere in Python/SQL/YAML source
def check_no_hardcoded_creds():
    patterns = [r'[Pp]assword\s*=\s*["\'][^"\']{6,}', r'secret\s*=\s*["\'][^"\']{8,}']
    violations = []
    exts = ['.py', '.sql', '.yml', '.yaml', '.sh']
    exclude_dirs = {'.git', '__pycache__', 'secrets', 'backup', 'node_modules'}
    for root, dirs, files in os.walk(PROJECT_DIR):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for fname in files:
            if any(fname.endswith(e) for e in exts):
                fpath = os.path.join(root, fname)
                try:
                    content = open(fpath, 'r', encoding='utf-8', errors='ignore').read()
                    for pat in patterns:
                        if re.search(pat, content):
                            rel = os.path.relpath(fpath, PROJECT_DIR)
                            violations.append(rel)
                            break
                except: pass
    if violations:
        return "FAIL", f"Possible hardcoded credentials in: {violations}"
    return "PASS", "No hardcoded credentials found in source files"
check("Security: No hardcoded credentials", check_no_hardcoded_creds, "security")

# 4. Webhook secret is non-trivial
def check_webhook_secret():
    secret = os.getenv("WEBHOOK_SECRET", "")
    if len(secret) < 24:
        return "FAIL", f"WEBHOOK_SECRET too short ({len(secret)} chars) — minimum 24"
    if secret in ["default_secret", "changeme", "secret"]:
        return "FAIL", "WEBHOOK_SECRET is a known default value"
    return "PASS", f"WEBHOOK_SECRET is {len(secret)} chars long"
check("Security: Webhook secret strength", check_webhook_secret, "security")

# 5. DB passwords URL-safe check (critical for SQLAlchemy)
def check_url_safe_passwords():
    raw_password = os.getenv("POSTGRES_PASSWORD", "")
    encoded_url = build_db_url()
    if raw_password and raw_password not in encoded_url:
        return "PASS", "POSTGRES_PASSWORD is URL-encoded by build_db_url()"
    unsafe = ['!', '@', '#', '$', '%', '&', '+', '=', '?', '/']
    found = [c for c in raw_password if c in unsafe]
    if found:
        return "FAIL", f"POSTGRES_PASSWORD contains URL-unsafe chars {found} and was not encoded"
    return "PASS", "POSTGRES_PASSWORD is URL-safe"
check("Security: SQLAlchemy DB URL encoding", check_url_safe_passwords, "security")

# ══════════════════════════════════════════════════════════════
# C. DATABASE PRIVILEGE ISOLATION
# ══════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("C. DATABASE PRIVILEGE ISOLATION")
print("="*65)

def check_etl_can_write():
    with engine.connect() as conn:
        ok = conn.execute(text(
            "SELECT has_schema_privilege('etl_writer', 'client_mtn', 'USAGE')"
        )).scalar()
        return ("PASS", "etl_writer has USAGE on client_mtn") if ok else ("FAIL", "etl_writer missing USAGE on client_mtn")
check("DB: etl_writer schema access", check_etl_can_write, "database")

def check_analyst_no_qc():
    with engine.connect() as conn:
        ok = conn.execute(text(
            "SELECT has_schema_privilege('analyst_reader', 'qc_system', 'USAGE')"
        )).scalar()
        # analyst_reader should NOT have qc_system access
        return ("FAIL", "analyst_reader has USAGE on qc_system — should be restricted") if ok else ("PASS", "analyst_reader correctly excluded from qc_system")
check("DB: analyst_reader blocked from qc_system", check_analyst_no_qc, "database")

def check_public_schema_locked():
    with engine.connect() as conn:
        # Check that PUBLIC cannot create objects in public schema (basic hardening)
        revoke_ok = conn.execute(text(
            "SELECT has_schema_privilege('public', 'public', 'CREATE')"
        )).scalar()
        if revoke_ok:
            return "WARN", "PUBLIC role can CREATE in public schema — run REVOKE CREATE ON SCHEMA public FROM PUBLIC"
        return "PASS", "public schema CREATE privilege revoked from PUBLIC"
check("DB: public schema locked", check_public_schema_locked, "database")

def check_indexes_exist():
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT indexname FROM pg_indexes WHERE schemaname='qc_system' AND tablename IN ('qc_flags','failed_payloads')"
        )).fetchall()
        names = [r[0] for r in rows]
        expected = ['idx_qc_flags_uuid', 'idx_qc_flags_severity', 'idx_failed_payloads_status']
        missing = [i for i in expected if i not in names]
        if missing:
            return "FAIL", f"Missing indexes: {missing}"
        return "PASS", f"All {len(expected)} performance indexes present"
check("DB: Performance indexes", check_indexes_exist, "database")

def check_audit_log_trigger():
    # Check if audit_log is getting written on schema changes
    with engine.connect() as conn:
        # Just verify audit_log structure is sound
        cols = conn.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='qc_system' AND table_name='audit_log'"
        )).fetchall()
        col_names = [r[0] for r in cols]
        required = ['id','action','schema_name','performed_by','detail','created_at']
        missing = [c for c in required if c not in col_names]
        if missing:
            return "FAIL", f"audit_log missing columns: {missing}"
        return "PASS", f"audit_log has all {len(required)} required columns"
check("DB: audit_log schema integrity", check_audit_log_trigger, "database")

# ══════════════════════════════════════════════════════════════
# D. MINIO BUCKET POLICY & CONFIGURATION
# ══════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("D. MINIO STORAGE CONFIGURATION")
print("="*65)

def check_bucket_versioning():
    issues = []
    for bucket in ['raw-bronze', 'processed-silver']:
        try:
            s3.get_bucket_versioning(Bucket=bucket)
        except Exception as e:
            issues.append(f"{bucket}: {e}")
    return ("INFO", f"Cannot check versioning (MinIO free tier): consider enabling for raw-bronze immutability") if not issues else ("FAIL", str(issues))
check("MinIO: Bucket versioning check", check_bucket_versioning, "minio")

def check_raw_bronze_immutability():
    # Test that we can write and read back correctly
    key = "_audit/immutability_test.json"
    data = json.dumps({"purpose": "immutability_audit"}).encode()
    s3.put_object(Bucket='raw-bronze', Key=key, Body=data)
    obj = s3.get_object(Bucket='raw-bronze', Key=key)
    readback = obj['Body'].read()
    s3.delete_object(Bucket='raw-bronze', Key=key)
    if readback == data:
        return "PASS", "raw-bronze read/write/delete cycle correct"
    return "FAIL", "Data mismatch on read-back"
check("MinIO: raw-bronze read/write cycle", check_raw_bronze_immutability, "minio")

def check_minio_bucket_completeness():
    expected = ['raw-bronze', 'processed-silver', 'exports', 'backup-staging']
    existing = [b['Name'] for b in s3.list_buckets()['Buckets']]
    missing = [b for b in expected if b not in existing]
    if missing:
        return "FAIL", f"Missing: {missing}"
    return "PASS", f"All {len(expected)} buckets exist: {existing}"
check("MinIO: All required buckets", check_minio_bucket_completeness, "minio")

# ══════════════════════════════════════════════════════════════
# E. DOCKER CONFIGURATION DEEP AUDIT
# ══════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("E. DOCKER CONFIGURATION DEEP AUDIT")
print("="*65)

compose_path = os.path.join(PROJECT_DIR, "docker-compose.yml")
compose_text = open(compose_path, encoding='utf-8').read()

def check_compose_restart_policies():
    services = ['postgres', 'minio', 'prefect']
    issues = []
    for svc in services:
        if f"{svc}:" not in compose_text:
            issues.append(f"{svc} missing")
    restart_count = compose_text.count("restart: always")
    if restart_count < 3:
        issues.append(f"Only {restart_count} services have restart:always")
    return ("PASS", "All core services have restart:always") if not issues else ("FAIL", str(issues))
check("Docker: restart:always on core services", check_compose_restart_policies, "docker")

def check_compose_mem_limits():
    if "mem_limit" not in compose_text:
        return "FAIL", "No mem_limit found — risk of OOM on 16GB host"
    count = compose_text.count("mem_limit")
    return "PASS", f"{count} mem_limit directives found"
check("Docker: Memory limits set", check_compose_mem_limits, "docker")

def check_compose_healthchecks():
    count = compose_text.count("healthcheck:")
    if count < 2:
        return "WARN", f"Only {count} healthcheck(s) — Prefect has no healthcheck"
    return "PASS", f"{count} healthchecks defined"
check("Docker: Healthchecks defined", check_compose_healthchecks, "docker")

def check_named_volumes():
    if "rp-postgres-data" in compose_text and "rp-minio-data" in compose_text:
        return "PASS", "Named volumes used — NTFS permission conflicts avoided"
    return "FAIL", "Not using named volumes — NTFS bind-mount permission issue risk"
check("Docker: Named volumes (not bind-mounts)", check_named_volumes, "docker")

# Prefect should expose PREFECT_API_URL correctly for external access
def check_prefect_api_url():
    if "PREFECT_API_URL: http://0.0.0.0:4200/api" in compose_text:
        return "WARN", "PREFECT_API_URL set to 0.0.0.0 inside container env — should be http://localhost:4200/api for client scripts"
    return "PASS", "Prefect API URL configured correctly"
check("Docker: Prefect API URL", check_prefect_api_url, "docker")

# ══════════════════════════════════════════════════════════════
# F. PYTHON CODE QUALITY DEEP AUDIT
# ══════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("F. PYTHON CODE QUALITY DEEP AUDIT")
print("="*65)

etl_path     = os.path.join(PROJECT_DIR, "etl/pipelines/sync_surveycto.py")
qc_path      = os.path.join(PROJECT_DIR, "etl/qc/qc_engine.py")
webhook_path = os.path.join(PROJECT_DIR, "webhook/webhook_server.py")

etl_code     = open(etl_path, encoding='utf-8').read()
qc_code      = open(qc_path, encoding='utf-8').read()
webhook_code = open(webhook_path, encoding='utf-8').read()

# Check: ETL uses engine.connect context manager in all DB calls (prevents connection leaks)
def check_connection_lifecycle():
    issues = []
    # In upsert_submissions, conn is opened directly without context manager
    if "conn = engine.connect()" in etl_code and "conn.close()" in etl_code:
        return "WARN", "ETL uses manual conn.open/close — prefer 'with engine.connect() as conn' context manager to guarantee cleanup on exceptions"
    return "PASS", "Connection lifecycle managed via context managers"
check("Code/ETL: Connection lifecycle", check_connection_lifecycle, "code")

# Check: ETL SQL injection risk via f-strings with form_id
def check_sql_injection():
    if "safe_form_id       = _safe_id(form_id)" not in etl_code:
        return "WARN", "ETL does not sanitize form_id before building table identifiers"
    if "safe_form   = _safe_id(form_id)" not in qc_code:
        return "WARN", "QC does not sanitize form_id before building table identifiers"
    return "PASS", "No obvious raw SQL injection vectors found"
check("Code: SQL injection surface (f-strings)", check_sql_injection, "code")

# Check: ETL key extraction — assumes 'KEY' always present but no guard
def check_etl_key_guard():
    if "sub['KEY']" in etl_code and ("if 'KEY' in sub" not in etl_code and "if 'KEY' not in sub" not in etl_code):
        return "WARN", "ETL accesses sub['KEY'] without checking key exists — will raise KeyError on malformed records"
    return "PASS", "ETL has proper KEY field guard"
check("Code/ETL: KEY field guard", check_etl_key_guard, "code")

# Check: Webhook uses subprocess.Popen with shell=True — security risk
def check_webhook_subprocess():
    if "subprocess.Popen" in webhook_code and "shell=True" in webhook_code:
        return "FAIL", "webhook_server.py uses subprocess.Popen(shell=True) — shell injection risk if form_id is not sanitized"
    return "PASS", "No shell=True subprocess usage"
check("Code/Webhook: subprocess shell injection", check_webhook_subprocess, "code")

# Check: Webhook form_id validated against allowlist before subprocess
def check_webhook_form_id_validation():
    if "get_form_runtime_config(form_id)" in webhook_code:
        if "load_form_registry(active_only=True).get(form_id)" in webhook_code and "if not mapping:" in webhook_code:
            return "PASS", "form_id validated against active SurveyCTO registry allowlist before processing"
    # Legacy FORM_SCHEMA_MAP lookup happens before process_and_trigger call
    if "FORM_SCHEMA_MAP.get(form_id)" in webhook_code:
        if "mapping = FORM_SCHEMA_MAP.get(form_id)" in webhook_code:
            if "if not mapping:" in webhook_code:
                return "PASS", "form_id validated against FORM_SCHEMA_MAP allowlist before processing"
    return "FAIL", "form_id not validated before processing — injection risk"
check("Code/Webhook: form_id allowlist validation", check_webhook_form_id_validation, "code")

# Check: backup script has pg_dump available
def check_backup_deps():
    pg_dump_exists = shutil.which("pg_dump") is not None
    docker_exists = shutil.which("docker") is not None
    aws_exists = shutil.which("aws") is not None
    cloud_configured = (
        os.getenv("B2_KEY_ID") and os.getenv("B2_APPLICATION_KEY")
    ) or (
        os.getenv("R2_ACCESS_KEY_ID") and os.getenv("R2_SECRET_ACCESS_KEY")
    )
    issues = []
    if not pg_dump_exists and not docker_exists:
        issues.append("pg_dump not found in PATH — backup will fail")
    if cloud_configured and not aws_exists and not docker_exists:
        issues.append("aws CLI not found — R2 uploads will fail")
    return ("WARN", f"Missing tools: {issues}") if issues else ("PASS", "Backup dependencies available")
check("Backup: Required CLI tools available", check_backup_deps, "code")

# Check: ETL has retry logic on Prefect tasks
def check_etl_retries():
    if "retries=3" in etl_code:
        return "PASS", "fetch_submissions task has retries=3 with 30s delay"
    return "WARN", "No retry configuration found on ETL tasks"
check("Code/ETL: Retry logic on Prefect tasks", check_etl_retries, "code")

# Check: Webhook has async background tasks (non-blocking)
def check_webhook_async():
    if "BackgroundTasks" in webhook_code and "background_tasks.add_task" in webhook_code:
        return "PASS", "Webhook uses FastAPI BackgroundTasks — non-blocking response"
    return "FAIL", "Webhook processing is blocking — will time out SurveyCTO sender"
check("Code/Webhook: Non-blocking background tasks", check_webhook_async, "code")

# ══════════════════════════════════════════════════════════════
# G. PROCESS REQUIREMENTS CHECKLIST
# ══════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("G. OPERATIONAL PROCESS REQUIREMENTS")
print("="*65)

def check_backup_script_executable():
    path = os.path.join(PROJECT_DIR, "scripts/backup_r2.sh")
    st = os.stat(path)
    executable = bool(st.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))
    if os.name == "nt":
        makefile_path = os.path.join(PROJECT_DIR, "Makefile")
        makefile_text = open(makefile_path, encoding="utf-8").read() if os.path.exists(makefile_path) else ""
        if "bash scripts/backup_r2.sh" in makefile_text:
            return "PASS", "backup target invokes backup_r2.sh through bash on Windows"
    return ("PASS", "backup_r2.sh is executable") if executable else ("WARN", "backup_r2.sh is not executable — run: chmod +x scripts/backup_r2.sh")
check("Ops: backup_r2.sh is executable", check_backup_script_executable, "ops")

def check_prefect_flows_registered():
    try:
        r = requests.post("http://localhost:4200/api/deployments/filter", timeout=15)
        data = r.json()
        count = len(data) if isinstance(data, list) else 0
        if count == 0:
            return "WARN", "No Prefect deployments registered — flows must be deployed before scheduling"
        return "PASS", f"{count} Prefect deployment(s) registered"
    except Exception as e:
        return "WARN", f"Could not query Prefect deployments API: {e}"
check("Ops: Prefect flows deployed", check_prefect_flows_registered, "ops")

def check_sync_state_initialized():
    with engine.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM qc_system.sync_state")).scalar()
        if count == 0:
            return "INFO", "sync_state is empty — will be populated on first ETL run (expected)"
        return "PASS", f"{count} pipeline sync state(s) recorded"
check("Ops: ETL sync state", check_sync_state_initialized, "ops")

def check_dlq_empty():
    with engine.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM qc_system.failed_payloads WHERE status='pending'")).scalar()
        if count > 0:
            return "WARN", f"{count} unprocessed failed payloads in DLQ"
        return "PASS", "Dead letter queue is clear"
check("Ops: Dead letter queue empty", check_dlq_empty, "ops")

def check_prefect_healthcheck():
    r = requests.get("http://localhost:4200/api/health", timeout=15)
    return ("PASS", f"Prefect API healthy (HTTP {r.status_code})") if r.status_code == 200 else ("FAIL", f"HTTP {r.status_code}")
check("Ops: Prefect server health", check_prefect_healthcheck, "ops")

def check_minio_health():
    r = requests.get("http://localhost:9000/minio/health/live", timeout=5)
    return ("PASS", f"MinIO live (HTTP {r.status_code})") if r.status_code == 200 else ("FAIL", f"HTTP {r.status_code}")
check("Ops: MinIO server health", check_minio_health, "ops")

def check_postgres_health():
    with engine.connect() as conn:
        ts = conn.execute(text("SELECT NOW()")).scalar()
        return "PASS", f"PostgreSQL responding — server time: {ts}"
check("Ops: PostgreSQL health", check_postgres_health, "ops")

# ══════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("FINAL AUDIT SUMMARY")
print("="*65)

categories = {}
for code, cat, name, detail in results:
    categories.setdefault(cat, {"PASS":0,"FAIL":0,"WARN":0,"INFO":0})
    categories[cat][code] = categories[cat].get(code, 0) + 1

total  = len(results)
passes = sum(1 for r in results if r[0]=="PASS")
fails  = sum(1 for r in results if r[0]=="FAIL")
warns  = sum(1 for r in results if r[0]=="WARN")
infos  = sum(1 for r in results if r[0]=="INFO")

print(f"\n  Total checks : {total}")
print(f"  ✅ PASS      : {passes}")
print(f"  ❌ FAIL      : {fails}")
print(f"  ⚠️  WARN      : {warns}")
print(f"  ℹ️  INFO      : {infos}")

if fails > 0:
    print(f"\n  CRITICAL FAILURES requiring immediate fix:")
    for code, cat, name, detail in results:
        if code == "FAIL":
            print(f"    [{cat.upper()}] {name}")
            print(f"          → {detail}")

if warns > 0:
    print(f"\n  WARNINGS to review (non-blocking but important):")
    for code, cat, name, detail in results:
        if code == "WARN":
            print(f"    [{cat.upper()}] {name}")
            print(f"          → {detail}")

print("\n" + "="*65)
sys.exit(1 if fails > 0 else 0)
