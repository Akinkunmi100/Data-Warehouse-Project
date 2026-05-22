#!/usr/bin/env python3
"""
Research Data Platform — Create Mock Table for Local Development

Creates a minimal client_mtn.project_appraise table so that dbt models and QC
checks can be tested locally before the first real SurveyCTO ETL run.

Usage (from project root in WSL2):
    python3 scripts/create_mock_table.py

FIX: was hardcoding the production password directly in the connection string.
     Now reads credentials from secrets/.env via python-dotenv — same pattern
     as every other script in the platform.
"""
import os
import sys

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, "secrets", ".env"))

from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5435")
DB_URL  = f"postgresql://{quote_plus(DB_USER)}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URL)

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS client_mtn.project_appraise (
            submission_uuid  TEXT PRIMARY KEY,
            "CompletionDate" TEXT,
            "SubmissionDate" TEXT,
            starttime        TEXT,
            endtime          TEXT,
            deviceid         TEXT,
            enumerator_id    TEXT,
            respondent_phone TEXT,
            submission_date  DATE,
            duration_seconds INTEGER,
            latitude         NUMERIC,
            longitude        NUMERIC,
            region           TEXT,
            review_status    TEXT,
            updated_at       TIMESTAMPTZ DEFAULT NOW()
        )
    """))
    conn.commit()
    print("Created (or confirmed) mock table: client_mtn.project_appraise")
