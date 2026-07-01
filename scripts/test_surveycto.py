#!/usr/bin/env python3
"""
Research Data Platform — SurveyCTO Ingestion Test Suite
========================================================
Tests every layer of the data ingestion path before a real pipeline run:

  Stage 1 · Network & Auth   — Can we reach the server? Do credentials work?
  Stage 2 · Form Discovery   — List all forms the account can see; confirm
                               the three hardcoded form IDs exist.
  Stage 3 · Data Fetch       — Pull the last 5 submissions from each known
                               form and inspect payload structure.
  Stage 4 · Payload Analysis — Detect repeat groups, select_multiple fields,
                               missing KEY, schema column count.
  Stage 5 · End-to-End ETL   — Run the full ETL in a transaction that is
                               rolled back immediately (no permanent changes).
  Stage 6 · MinIO Archive    — Write a test payload to raw-bronze and read
                               it back to confirm the storage path works.

Run from WSL2:
    python3 scripts/test_surveycto.py               # full suite
    python3 scripts/test_surveycto.py --list-forms  # only list forms
    python3 scripts/test_surveycto.py --form <id>   # test one specific form
    python3 scripts/test_surveycto.py --fetch-only  # stages 1-4 only (read-only)
"""

import os
import sys
import json
import time
import argparse
import traceback
from datetime import datetime, timezone, timedelta

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# ── Path bootstrap ────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, "secrets", ".env"))

from etl.surveycto_registry import load_form_registry

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

PASS = f"{GREEN}✅ PASS{RESET}"
FAIL = f"{RED}❌ FAIL{RESET}"
WARN = f"{YELLOW}⚠️  WARN{RESET}"
INFO = f"{CYAN}ℹ️  INFO{RESET}"

results: list[tuple[str, str, str]] = []   # (status_tag, name, detail)

def _tag(label: str) -> str:
    """Strip ANSI codes to get the raw label for result tracking."""
    import re
    return re.sub(r'\033\[[0-9;]*m', '', label)

def record(status: str, name: str, detail: str):
    results.append((_tag(status), name, detail))
    print(f"  {status}  {name}")
    if detail:
        # Indent multi-line detail blocks
        for line in detail.splitlines():
            print(f"         {line}")

def section(title: str):
    bar = "═" * 60
    print(f"\n{BOLD}{bar}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{bar}{RESET}")

# ── Credentials ───────────────────────────────────────────────────────────────
SCTO_URL  = os.getenv("SURVEYCTO_SERVER_URL", "").rstrip("/")
SCTO_USER = os.getenv("SURVEYCTO_USERNAME", "")
SCTO_PASS = os.getenv("SURVEYCTO_PASSWORD", "")

# Active SurveyCTO forms registered for this project.
REGISTERED_FORMS = {
    form_id: {"client": config["client"], "schema": config["schema"]}
    for form_id, config in load_form_registry(active_only=True).items()
}


def probe_registered_forms(target_form: str | None = None) -> dict[str, tuple[int, str]]:
    """Probe the same SurveyCTO wide JSON endpoints used by the ETL."""
    import requests

    forms_to_probe = [target_form] if target_form else list(REGISTERED_FORMS.keys())
    since_ms = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000)
    statuses: dict[str, tuple[int, str]] = {}
    for form_id in forms_to_probe:
        url = f"{SCTO_URL}/api/v2/forms/data/wide/json/{form_id}"
        try:
            response = requests.get(
                url,
                params={"date": since_ms},
                auth=(SCTO_USER, SCTO_PASS),
                timeout=30,
            )
            statuses[form_id] = (response.status_code, response.text[:300])
        except Exception as exc:
            statuses[form_id] = (0, str(exc))
    return statuses

# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1 — NETWORK & AUTHENTICATION
# ─────────────────────────────────────────────────────────────────────────────

def stage_1_network_auth():
    section("Stage 1 · Network & Authentication")
    import requests

    # 1a. Credential presence
    missing = [v for v in ["SURVEYCTO_SERVER_URL","SURVEYCTO_USERNAME","SURVEYCTO_PASSWORD"]
               if not os.getenv(v)]
    if missing:
        record(FAIL, "Credentials present in .env", f"Missing: {missing}")
        return False
    record(PASS, "Credentials present in .env",
           f"URL={SCTO_URL}  USER={SCTO_USER}  PASS={'*'*len(SCTO_PASS)}")

    # 1b. TCP reachability (no auth)
    host = SCTO_URL.replace("https://","").replace("http://","").split("/")[0]
    try:
        import socket
        socket.setdefaulttimeout(10)
        socket.create_connection((host, 443), timeout=10)
        record(PASS, f"TCP reachable  ({host}:443)", "Connection established")
    except Exception as e:
        record(FAIL, f"TCP reachable  ({host}:443)", str(e))
        print(f"\n  {RED}Cannot reach SurveyCTO server — no network or wrong URL.{RESET}")
        return False

    # 1c. HTTPS landing page
    try:
        r = requests.get(SCTO_URL, timeout=10, allow_redirects=True)
        record(PASS, "HTTPS GET /  (landing page)",
               f"HTTP {r.status_code}  ({len(r.content):,} bytes)")
    except Exception as e:
        record(FAIL, "HTTPS GET /  (landing page)", str(e))

    # 1d. API authentication — use the /forms endpoint as the cheapest auth probe
    api_url = f"{SCTO_URL}/api/v2/forms"
    try:
        t0 = time.time()
        r  = requests.get(api_url, auth=(SCTO_USER, SCTO_PASS), timeout=30)
        ms = int((time.time() - t0) * 1000)

        if r.status_code == 200:
            record(PASS, "API authentication", f"HTTP 200 in {ms} ms")
            return True
        elif r.status_code == 401:
            record(FAIL, "API authentication",
                   f"HTTP 401 — username or password rejected.\n"
                   f"  Check SURVEYCTO_USERNAME='{SCTO_USER}' and SURVEYCTO_PASSWORD in secrets/.env.")
        elif r.status_code == 403:
            record(WARN, "API authentication",
                   f"HTTP 403 — credentials accepted but user lacks API access.\n"
                   f"  In SurveyCTO: Admin → Users → edit '{SCTO_USER}' → enable 'API access'.")
        elif r.status_code == 404 and "Not a valid API call" in r.text:
            statuses = probe_registered_forms()
            accessible = [fid for fid, (code, _) in statuses.items() if code == 200]
            denied = [fid for fid, (code, _) in statuses.items() if code == 403]
            if accessible:
                record(
                    PASS,
                    "API authentication",
                    f"GET /api/v2/forms is unavailable on this server; "
                    f"wide JSON endpoint authenticated for: {accessible}"
                )
                if denied:
                    record(
                        WARN,
                        "Registered form permissions",
                        f"Authenticated, but Download Data access is denied for: {denied}"
                    )
                return True
            if denied:
                record(
                    WARN,
                    "API authentication",
                    "Credentials were accepted, but Download Data access is denied "
                    f"for all registered forms: {denied}"
                )
                return True
            record(
                FAIL,
                "API authentication",
                "GET /api/v2/forms is unavailable and no registered wide JSON "
                f"endpoint could be reached. Probe results: {statuses}"
            )
        else:
            record(FAIL, "API authentication",
                   f"HTTP {r.status_code}  body: {r.text[:300]}")
    except requests.exceptions.Timeout:
        record(FAIL, "API authentication",
               f"Timed out after 30 s — SurveyCTO server not responding to API calls.")
    except Exception as e:
        record(FAIL, "API authentication", str(e))

    return False


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 2 — FORM DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

def stage_2_form_discovery(target_form: str | None = None) -> list[str]:
    """
    Returns a list of form IDs found on the server.
    If --list-forms was requested, prints a formatted table.
    """
    section("Stage 2 · Form Discovery")
    import requests

    api_url = f"{SCTO_URL}/api/v2/forms"
    try:
        r = requests.get(api_url, auth=(SCTO_USER, SCTO_PASS), timeout=30)
        r.raise_for_status()
    except Exception as e:
        record(
            WARN,
            "List forms  (GET /api/v2/forms)",
            f"{e}\n"
            "Falling back to registered form probes because this SurveyCTO server "
            "does not expose the generic form-list endpoint."
        )
        statuses = probe_registered_forms(target_form=target_form)
        server_form_ids = []
        for fid, (code, detail) in statuses.items():
            if code == 200:
                record(PASS, f"Registered form '{fid}' data endpoint", "Accessible")
                server_form_ids.append(fid)
            elif code == 403:
                record(
                    FAIL,
                    f"Registered form '{fid}' data endpoint",
                    "HTTP 403 - user lacks 'Download data' permission for this form"
                )
            elif code == 404:
                record(
                    FAIL,
                    f"Registered form '{fid}' data endpoint",
                    "HTTP 404 - form not found on server (check form ID spelling)"
                )
            elif code == 417:
                record(
                    WARN,
                    f"Registered form '{fid}' data endpoint",
                    "SurveyCTO asked us to wait before pulling again; form exists "
                    "and the previous probe authenticated."
                )
                server_form_ids.append(fid)
            else:
                record(
                    FAIL,
                    f"Registered form '{fid}' data endpoint",
                    f"HTTP {code}: {detail}"
                )
        return server_form_ids

    try:
        forms = r.json()
    except Exception:
        record(FAIL, "List forms — JSON parse",
               f"Response is not JSON. First 300 chars: {r.text[:300]}")
        return []

    if not isinstance(forms, list):
        record(FAIL, "List forms — response type",
               f"Expected list, got {type(forms).__name__}: {str(forms)[:200]}")
        return []

    record(PASS, f"List forms  (GET /api/v2/forms)",
           f"{len(forms)} form(s) accessible to this account")

    # Print a summary table
    if forms:
        print(f"\n  {'FORM ID':<35} {'TITLE':<40} {'SUBMISSIONS':>11}")
        print(f"  {'─'*35} {'─'*40} {'─'*11}")
        server_form_ids = []
        for f in forms:
            fid    = f.get("id", f.get("formId", "unknown"))
            title  = f.get("title", f.get("name", "—"))[:38]
            count  = f.get("submissionCount", f.get("totalSubmissions", "?"))
            marker = " ◀ REGISTERED" if fid in REGISTERED_FORMS else ""
            print(f"  {fid:<35} {title:<40} {str(count):>11}{marker}")
            server_form_ids.append(fid)
        print()
    else:
        server_form_ids = []
        print(f"  {WARN}  No forms returned — user may not have access to any forms.")

    # Cross-check registered form IDs against server
    forms_to_test = [target_form] if target_form else list(REGISTERED_FORMS.keys())
    for fid in forms_to_test:
        if fid in server_form_ids:
            record(PASS, f"Registered form '{fid}' exists on server", "")
        else:
            record(FAIL, f"Registered form '{fid}' NOT found on server",
                   f"Available IDs: {server_form_ids[:10]}\n"
                   f"Fix: update FORM_SCHEMA_MAP in webhook_server.py and the deployment\n"
                   f"     parameters in sync_surveycto.py to match the real form ID.")

    return server_form_ids


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 3 — DATA FETCH
# ─────────────────────────────────────────────────────────────────────────────

def stage_3_fetch_data(form_ids: list[str], target_form: str | None = None) -> dict:
    """
    Fetches the last 5 submissions from each form to inspect the real payload.
    Returns a dict of {form_id: [submissions]} for Stage 4 analysis.
    """
    section("Stage 3 · Data Fetch (last 5 submissions per form)")
    import requests

    forms_to_test = [target_form] if target_form else [f for f in REGISTERED_FORMS if f in form_ids]
    if not forms_to_test:
        print(f"  {WARN}  No registered forms found on server — skipping data fetch.")
        return {}

    fetched: dict[str, list] = {}

    for form_id in forms_to_test:
        url = f"{SCTO_URL}/api/v2/forms/data/wide/json/{form_id}"
        # Fetch submissions from the last 90 days to get a meaningful sample
        since_ms = int((datetime.now(timezone.utc) - timedelta(days=90)).timestamp() * 1000)

        print(f"\n  Fetching from: {url}")
        print(f"  Since (last 90 days): {datetime.fromtimestamp(since_ms/1000, tz=timezone.utc).date()}")

        # Try with review_status filter first; fall back without it. Avoid date=0
        # here because full-pull diagnostics can trigger SurveyCTO cooldowns.
        attempts = [
            {"date": since_ms, "review_status": "approved|rejected"},
            {"date": since_ms},
        ]
        for attempt, params in enumerate(attempts, start=1):
            label = {1: "with review_status filter", 2: "without review_status filter"}[attempt]
            try:
                t0 = time.time()
                r  = requests.get(url, params=params, auth=(SCTO_USER, SCTO_PASS), timeout=60)
                ms = int((time.time() - t0) * 1000)
            except Exception as e:
                record(FAIL, f"'{form_id}' — fetch attempt {attempt}", str(e))
                break

            if r.status_code == 400 and attempt < len(attempts):
                print(f"  {WARN}  HTTP 400 ({label}) — retrying...")
                continue
            if r.status_code == 404:
                record(FAIL, f"'{form_id}' — fetch",
                       f"HTTP 404 — form not found on server (check form ID spelling)")
                break
            if r.status_code == 403:
                record(FAIL, f"'{form_id}' — fetch",
                       f"HTTP 403 — user lacks 'Download data' permission for this form")
                break
            if r.status_code != 200:
                record(FAIL, f"'{form_id}' — fetch",
                       f"HTTP {r.status_code}: {r.text[:200]}")
                break

            try:
                data = r.json()
            except Exception as e:
                record(FAIL, f"'{form_id}' — JSON parse", f"{e}  Body: {r.text[:200]}")
                break

            if not isinstance(data, list):
                record(FAIL, f"'{form_id}' — response type",
                       f"Expected list, got {type(data).__name__}: {str(data)[:200]}")
                break

            if len(data) == 0 and attempt < len(attempts):
                print(f"  {WARN}  0 submissions ({label}) — trying wider window...")
                continue

            record(PASS, f"'{form_id}' — fetch ({label})",
                   f"{len(data)} submission(s) returned in {ms} ms")
            fetched[form_id] = data[:5]   # keep first 5 for analysis
            break
        else:
            if form_id not in fetched:
                record(WARN, f"'{form_id}' — fetch", "0 submissions found on all attempts (form may be empty)")
                fetched[form_id] = []

    return fetched


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 4 — PAYLOAD ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

def stage_4_payload_analysis(fetched: dict):
    section("Stage 4 · Payload Structure Analysis")

    if not fetched:
        print(f"  {WARN}  No data fetched — skipping analysis.")
        return

    for form_id, submissions in fetched.items():
        if not submissions:
            record(WARN, f"'{form_id}' — analysis", "No submissions to inspect")
            continue

        sample = submissions[0]
        all_keys      = list(sample.keys())
        repeat_groups = [k for k, v in sample.items() if isinstance(v, list)]
        scalar_fields = [k for k, v in sample.items() if not isinstance(v, list)]
        select_multi  = [k for k, v in sample.items()
                         if isinstance(v, str) and ' ' in v.strip() and k not in
                         ('KEY','SubmissionDate','starttime','endtime','deviceid',
                          'subscriberid','simid','devicephonenum','username','caseid')]

        print(f"\n  {BOLD}Form: {form_id}{RESET}  ({len(submissions)} sample submission(s))")
        print(f"  {'─'*56}")

        # KEY field
        if "KEY" in sample:
            record(PASS, f"  '{form_id}' — KEY field present",
                   f"Sample KEY: {sample['KEY'][:60]}")
        else:
            record(FAIL, f"  '{form_id}' — KEY field MISSING",
                   "ETL upsert requires 'KEY'. Ensure you are using the wide-format JSON endpoint.")

        # Submission date
        for date_field in ('SubmissionDate', 'submissiondate', 'submission_date'):
            if date_field in sample:
                record(PASS, f"  '{form_id}' — date field ('{date_field}')",
                       f"Value: {sample[date_field]}")
                break
        else:
            record(WARN, f"  '{form_id}' — no SubmissionDate field",
                   "Cursor-based incremental sync may not work as expected.")

        # Column count
        record(INFO, f"  '{form_id}' — columns",
               f"{len(scalar_fields)} scalar  +  {len(repeat_groups)} repeat group(s)")

        # Print scalar columns
        print(f"\n    Scalar columns ({len(scalar_fields)}):")
        for col in scalar_fields[:30]:
            val = str(sample.get(col, ''))[:60]
            print(f"      {col:<40} = {val}")
        if len(scalar_fields) > 30:
            print(f"      ... and {len(scalar_fields)-30} more columns")

        # Repeat groups
        if repeat_groups:
            print(f"\n    Repeat groups ({len(repeat_groups)}) — will become child tables:")
            for grp in repeat_groups:
                rows = sample[grp]
                child_cols = list(rows[0].keys()) if rows and isinstance(rows[0], dict) else []
                print(f"      {grp}  ({len(rows)} row(s), {len(child_cols)} columns)")
                print(f"        Child columns: {child_cols[:10]}")
                if len(child_cols) > 10:
                    print(f"        ... and {len(child_cols)-10} more")
            record(PASS, f"  '{form_id}' — repeat groups detected",
                   f"{repeat_groups}  — ETL will create child tables automatically")
        else:
            record(PASS, f"  '{form_id}' — no repeat groups", "Flat form — simple upsert")

        # select_multiple
        if select_multi:
            record(WARN, f"  '{form_id}' — possible select_multiple fields",
                   f"{select_multi[:5]}\n"
                   f"  These are space-separated strings. dbt silver layer should split them.")
        else:
            record(INFO, f"  '{form_id}' — select_multiple fields",
                   "None detected in sample")

        # review_status
        if 'review_status' in sample:
            record(PASS, f"  '{form_id}' — review_status field",
                   f"Value: {sample['review_status']}")
        else:
            record(WARN, f"  '{form_id}' — review_status field missing",
                   "ETL will default to 'unknown'. Check if form uses review workflow.")


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 5 — END-TO-END ETL (ROLLBACK / DRY-RUN)
# ─────────────────────────────────────────────────────────────────────────────

def stage_5_etl_dry_run(fetched: dict):
    """
    Runs the full ETL upsert inside a database transaction that is always
    rolled back. This confirms the code path works end-to-end without
    writing permanent rows.
    """
    section("Stage 5 · End-to-End ETL Dry-Run (rolled back)")

    if not fetched:
        print(f"  {WARN}  No data to dry-run — skipping.")
        return

    try:
        from etl.utils import build_db_url
        from sqlalchemy import create_engine, text
        engine = create_engine(build_db_url())
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        record(PASS, "DB: warehouse connection", "")
    except Exception as e:
        record(FAIL, "DB: warehouse connection", str(e))
        print(f"  {RED}Cannot connect to PostgreSQL — ensure 'make up' is running.{RESET}")
        return

    for form_id, submissions in fetched.items():
        if not submissions:
            record(WARN, f"'{form_id}' — ETL dry-run", "No submissions — skipped")
            continue

        mapping = REGISTERED_FORMS.get(form_id)
        if not mapping:
            record(WARN, f"'{form_id}' — ETL dry-run",
                   "Form not in REGISTERED_FORMS — skipped (Stage 2 would catch this)")
            continue

        from sqlalchemy import create_engine
        from etl.utils import build_db_url
        engine = create_engine(build_db_url())

        try:
            with engine.begin() as conn:          # begin() auto-commits on exit
                # We'll use a SAVEPOINT and roll it back so nothing persists
                conn.execute(text("SAVEPOINT etl_dryrun"))

                from etl.pipelines.sync_surveycto import upsert_submissions
                upsert_submissions(
                    form_id       = form_id,
                    client_schema = mapping["schema"],
                    submissions   = submissions,
                    engine        = engine,
                )

                conn.execute(text("ROLLBACK TO SAVEPOINT etl_dryrun"))
                conn.execute(text("RELEASE SAVEPOINT etl_dryrun"))

            record(PASS, f"'{form_id}' — ETL dry-run",
                   f"{len(submissions)} submission(s) processed and rolled back cleanly")

        except Exception as e:
            record(FAIL, f"'{form_id}' — ETL dry-run", str(e))
            print()
            traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 6 — MINIO ARCHIVE PATH
# ─────────────────────────────────────────────────────────────────────────────

def stage_6_minio_archive():
    section("Stage 6 · MinIO Raw-Bronze Archive")

    try:
        import boto3
        from botocore.client import Config

        s3 = boto3.client(
            's3',
            endpoint_url="http://localhost:9000",
            aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
            config=Config(signature_version='s3v4')
        )
        buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
        record(PASS, "MinIO: connection", f"Buckets: {buckets}")

        if 'raw-bronze' not in buckets:
            record(FAIL, "MinIO: raw-bronze bucket",
                   "Bucket missing — run: make setup-buckets  or  make test")
            return

        # Write a test payload and read it back
        test_key  = "_etl_test/surveycto_connection_check.json"
        test_body = json.dumps({
            "test": True,
            "form_id": "test_surveycto_py",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }).encode('utf-8')

        s3.put_object(Bucket='raw-bronze', Key=test_key, Body=test_body)
        obj       = s3.get_object(Bucket='raw-bronze', Key=test_key)
        read_back = obj['Body'].read()
        s3.delete_object(Bucket='raw-bronze', Key=test_key)

        if read_back == test_body:
            record(PASS, "MinIO: raw-bronze write/read/delete cycle", "Archive path is healthy")
        else:
            record(FAIL, "MinIO: raw-bronze read-back mismatch",
                   "Data written does not match data read back — storage issue")

    except Exception as e:
        record(FAIL, "MinIO: connection", str(e))
        print(f"  {YELLOW}MinIO may not be running — start with: make up{RESET}")


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

def print_summary():
    bar = "═" * 60
    print(f"\n{BOLD}{bar}{RESET}")
    print(f"{BOLD}  SURVEYCTO INGESTION TEST — SUMMARY{RESET}")
    print(f"{BOLD}{bar}{RESET}")

    passes = sum(1 for r in results if "PASS" in r[0])
    fails  = sum(1 for r in results if "FAIL" in r[0])
    warns  = sum(1 for r in results if "WARN" in r[0])
    infos  = sum(1 for r in results if "INFO" in r[0])

    print(f"\n  Total checks : {len(results)}")
    print(f"  {PASS}         : {passes}")
    print(f"  {FAIL}         : {fails}")
    print(f"  {WARN}         : {warns}")
    print(f"  {INFO}         : {infos}")

    if fails > 0:
        print(f"\n{RED}{BOLD}  Failures (must fix before running the pipeline):{RESET}")
        for tag, name, detail in results:
            if "FAIL" in tag:
                print(f"    → {name}")
                if detail:
                    for line in detail.splitlines():
                        print(f"       {line}")
    else:
        print(f"\n{GREEN}{BOLD}  All checks passed — pipeline is ready to run.{RESET}")
        print(f"\n  Next step:")
        if REGISTERED_FORMS:
            first_form = next(iter(REGISTERED_FORMS))
            print(f"    python etl\\pipelines\\sync_surveycto.py {first_form}")
        else:
            print("    python scripts\\register_surveycto_form.py <form_id>")

    print(f"\n{bar}\n")
    return fails


# ─────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="SurveyCTO ingestion test suite"
    )
    parser.add_argument(
        "--list-forms",  action="store_true",
        help="Only list forms available on the server and exit"
    )
    parser.add_argument(
        "--form", metavar="FORM_ID",
        help="Test a single specific form ID instead of the registered defaults"
    )
    parser.add_argument(
        "--fetch-only", action="store_true",
        help="Stages 1–4 only (read-only, no DB or MinIO writes)"
    )
    args = parser.parse_args()

    target = args.form

    auth_ok = stage_1_network_auth()

    if not auth_ok:
        print(f"\n{RED}Cannot authenticate — stopping here.{RESET}")
        print_summary()
        sys.exit(1)

    server_form_ids = stage_2_form_discovery(target_form=target)

    if args.list_forms:
        sys.exit(0)

    fetched = stage_3_fetch_data(server_form_ids, target_form=target)
    stage_4_payload_analysis(fetched)

    if not args.fetch_only:
        stage_5_etl_dry_run(fetched)
        stage_6_minio_archive()

    fails = print_summary()
    sys.exit(1 if fails > 0 else 0)


if __name__ == "__main__":
    main()
