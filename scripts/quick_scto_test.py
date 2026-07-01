#!/usr/bin/env python3
"""
Minimal SurveyCTO connection test.
No Prefect, no database, no MinIO -- just requests.
Run from the project root:  python3 scripts/quick_scto_test.py
"""
import os, sys, json
from datetime import datetime, timezone, timedelta

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, "secrets", ".env"))
except ImportError:
    env_path = os.path.join(PROJECT_ROOT, "secrets", ".env")
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                v = v.strip().strip("'").strip('"')
                os.environ.setdefault(k.strip(), v)

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    print("requests not installed. Run: pip3 install requests")
    sys.exit(1)

SERVER   = os.getenv("SURVEYCTO_SERVER_URL", "").rstrip("/")
USERNAME = os.getenv("SURVEYCTO_USERNAME", "")
PASSWORD = os.getenv("SURVEYCTO_PASSWORD", "")

from etl.surveycto_registry import load_form_registry

FORMS = [
    (form_id, config["schema"])
    for form_id, config in load_form_registry(active_only=True).items()
]

SEP = "-" * 60

def banner(msg): print("\n" + "="*60 + "\n  " + msg + "\n" + "="*60)
def ok(msg):   print("  OK   " + msg)
def fail(msg): print("  FAIL " + msg)
def info(msg): print("  INFO " + msg)
def warn(msg): print("  WARN " + msg)

banner("1. Credentials")
ok("Server   : " + SERVER)
ok("Username : " + USERNAME)
masked = PASSWORD[:2] + "*"*(len(PASSWORD)-4) + PASSWORD[-2:] if len(PASSWORD) > 4 else "****"
ok("Password : " + masked + "  (" + str(len(PASSWORD)) + " chars)")
if not SERVER or not USERNAME or not PASSWORD:
    fail("Missing credentials -- check secrets/.env")
    sys.exit(1)

banner("2. Server reachability")
try:
    r = requests.get(SERVER, timeout=10)
    ok("Server reachable  (HTTP " + str(r.status_code) + ")")
except requests.exceptions.ConnectionError as e:
    fail("Cannot reach " + SERVER + "\n  " + str(e))
    sys.exit(1)
except requests.exceptions.Timeout:
    fail("Timed out reaching " + SERVER)
    sys.exit(1)

banner("3. Authentication")
auth = HTTPBasicAuth(USERNAME, PASSWORD)
far_future_ms = int(datetime(2099, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
probe_form = FORMS[0][0]
probe_url  = SERVER + "/api/v2/forms/data/wide/json/" + probe_form
try:
    r = requests.get(probe_url, params={"date": far_future_ms}, auth=auth, timeout=20)
except Exception as e:
    fail("Request failed: " + str(e)); sys.exit(1)

if r.status_code == 200:
    ok("Authenticated successfully  (HTTP 200)")
elif r.status_code == 401:
    fail("HTTP 401 -- wrong username or password"); sys.exit(1)
elif r.status_code == 403:
    fail("HTTP 403 -- user lacks API access in SurveyCTO Admin -> Users"); sys.exit(1)
elif r.status_code == 404:
    warn("HTTP 404 on probe form -- continuing to per-form tests")
else:
    warn("HTTP " + str(r.status_code) + " -- continuing anyway")

banner("4. Per-form data fetch")
yesterday_ms = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp() * 1000)
KEY_COLS = {"KEY","SubmissionDate","CompletionDate","starttime","endtime","deviceid","username","review_status"}
results  = {}
all_ok   = True

for form_id, schema in FORMS:
    print("\n  " + SEP)
    print("  Form: " + form_id + "  schema: " + schema)
    print("  " + SEP)

    url  = SERVER + "/api/v2/forms/data/wide/json/" + form_id
    data = None

    for attempt, params in enumerate([
        {"date": yesterday_ms, "review_status": "approved|rejected"},
        {"date": yesterday_ms},
    ], 1):
        try:
            r = requests.get(url, params=params, auth=auth, timeout=60)
        except requests.exceptions.Timeout:
            fail("  Timed out"); all_ok = False; break

        if r.status_code == 400 and attempt == 1:
            warn("  HTTP 400 with review_status filter -- retrying without"); continue
        if r.status_code == 404:
            fail("  HTTP 404 -- form not found on server"); all_ok = False; break
        if r.status_code == 403:
            fail("  HTTP 403 -- no permission"); all_ok = False; break
        if r.status_code != 200:
            fail("  HTTP " + str(r.status_code)); all_ok = False; break

        try:
            data = r.json()
        except Exception as e:
            fail("  Not JSON: " + str(e)); break

        if not isinstance(data, list):
            fail("  Expected array, got " + type(data).__name__); break

        ok("  HTTP 200  --  " + str(len(data)) + " submission(s) in last 24 h")

        if len(data) == 0:
            warn("  No submissions in last 24 h -- trying full fetch (date=0)...")
            try:
                r2 = requests.get(url, params={"date": 0}, auth=auth, timeout=60)
                if r2.status_code == 200:
                    all_data = r2.json()
                    if isinstance(all_data, list) and len(all_data) > 0:
                        ok("  Full fetch: " + str(len(all_data)) + " total submission(s)")
                        data = all_data
                    else:
                        warn("  Full fetch: 0 records -- form has no data yet")
            except Exception as e:
                warn("  Full fetch failed: " + str(e))

        if data:
            sample = data[0]
            scalar = {k: v for k, v in sample.items() if not isinstance(v, list)}
            groups = [k for k, v in sample.items() if isinstance(v, list)]
            print("\n  Columns (" + str(len(scalar)) + " scalar" +
                  (", " + str(len(groups)) + " repeat group(s): " + str(groups) if groups else "") + "):")
            for col, val in scalar.items():
                star = "* " if col in KEY_COLS else "  "
                val_str = str(val)[:60] if val not in (None, "") else "(empty)"
                print("    " + star + col.ljust(35) + " = " + repr(val_str))
            print()
            for col in sorted(KEY_COLS):
                case_match = next((k for k in scalar if k.lower() == col.lower()), None)
                if col in scalar:
                    ok("  " + col + " present")
                elif case_match:
                    warn("  " + col + " found as '" + case_match + "' (wrong casing)")
                else:
                    info("  " + col + " not present in this form")

        results[form_id] = data
        break

    if form_id not in results:
        results[form_id] = None

banner("5. Summary")
for form_id, data in results.items():
    if data is None:
        fail(form_id.ljust(25) + " FAILED")
    elif len(data) == 0:
        warn(form_id.ljust(25) + " OK -- 0 records")
    else:
        sample = data[0]
        ncols  = len([k for k,v in sample.items() if not isinstance(v,list)])
        ok(form_id.ljust(25) + str(len(data)) + " record(s), " + str(ncols) + " columns")

if all_ok:
    print("\n  Connection confirmed. To run a full ETL ingest:")
    print()
    print("  python3 -c \"")
    print("    import sys; sys.path.insert(0,'.')")
    print("    from dotenv import load_dotenv; load_dotenv('secrets/.env')")
    print("    from etl.pipelines.sync_surveycto import run_etl")
    print("    run_etl('project_appraise','client_mtn','client_mtn')\"")
else:
    print("\n  Some forms failed -- fix errors above before running ETL.")
print("="*60)
