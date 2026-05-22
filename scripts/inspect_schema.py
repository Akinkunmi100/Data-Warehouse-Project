#!/usr/bin/env python3
"""
Research Data Platform — Inspect Live Schema

Prints every table and its columns for each client schema.
Useful for verifying what the ETL actually created after a real sync run.

Usage (from project root in WSL2):
    python3 scripts/inspect_schema.py

FIX: was hardcoding the production password in the connection string.
     Now reads from secrets/.env.
FIX: was building SQL queries via f-string interpolation with table_name values
     from the DB — switched to parameterised queries.
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

SCHEMAS = ["client_mtn", "client_unilever", "internal", "qc_system", "gold", "silver", "bronze"]

with engine.connect() as conn:
    for schema in SCHEMAS:
        tables = conn.execute(
            text("SELECT table_name FROM information_schema.tables WHERE table_schema = :s ORDER BY table_name"),
            {"s": schema}
        ).fetchall()

        if not tables:
            continue

        print(f"\n{'='*55}")
        print(f"Schema: {schema}  ({len(tables)} table(s))")
        print(f"{'='*55}")

        for (table_name,) in tables:
            # FIX: parameterised query — table_name is not interpolated into SQL
            columns = conn.execute(
                text("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = :s AND table_name = :t
                    ORDER BY ordinal_position
                """),
                {"s": schema, "t": table_name}
            ).fetchall()

            print(f"\n  {schema}.{table_name}")
            for col_name, data_type, nullable in columns:
                null_marker = "" if nullable == "YES" else " NOT NULL"
                print(f"    {col_name:<35} {data_type}{null_marker}")
