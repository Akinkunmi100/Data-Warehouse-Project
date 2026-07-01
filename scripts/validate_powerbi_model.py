"""Validate and document the Power BI-facing warehouse contract."""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2


ROOT = Path(__file__).resolve().parents[1]
POWERBI_DIR = ROOT / "powerbi"

TABLES = [
    "dim_powerbi_project",
    "dim_powerbi_state",
    "fact_powerbi_state_quota",
    "fact_powerbi_project_daily",
    "fact_powerbi_qc_summary",
    "fact_powerbi_enumerator_scorecard",
    "fact_powerbi_duplicates",
    "fact_powerbi_regional_performance",
    "fact_powerbi_pipeline_health",
    "powerbi_project_risk_summary",
    "powerbi_kpi_snapshot",
]


def load_env() -> None:
    env_path = ROOT / "secrets" / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def db_kwargs() -> dict[str, object]:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5435")),
        "dbname": os.getenv("POSTGRES_DB", "warehouse"),
        "user": os.getenv("POSTGRES_USER", "platform_admin"),
        "password": os.environ["POSTGRES_PASSWORD"],
    }


def main() -> int:
    load_env()
    POWERBI_DIR.mkdir(exist_ok=True)
    lines = [
        "# Power BI Model Catalog",
        "",
        "Generated from live PostgreSQL metadata.",
        "",
    ]
    failures: list[str] = []
    with psycopg2.connect(**db_kwargs()) as conn:
        with conn.cursor() as cur:
            for table in TABLES:
                cur.execute(
                    """
                    select to_regclass(%s)
                    """,
                    (f"gold.{table}",),
                )
                if cur.fetchone()[0] is None:
                    failures.append(f"Missing table: gold.{table}")
                    continue

                cur.execute(f'select count(*) from gold."{table}"')
                row_count = cur.fetchone()[0]
                print(f"gold.{table}: {row_count} row(s)")
                lines.extend([f"## gold.{table}", "", f"Rows at validation time: `{row_count}`", ""])

                cur.execute(
                    """
                    select column_name, data_type, is_nullable
                      from information_schema.columns
                     where table_schema = 'gold'
                       and table_name = %s
                     order by ordinal_position
                    """,
                    (table,),
                )
                lines.extend(["| Column | Type | Nullable |", "|---|---|---|"])
                for column_name, data_type, is_nullable in cur.fetchall():
                    lines.append(f"| `{column_name}` | {data_type} | {is_nullable} |")
                lines.append("")

            cur.execute("select count(*) from qc_system.project_state_quotas")
            quota_rows = cur.fetchone()[0]
            if quota_rows == 0:
                print("NOTE: qc_system.project_state_quotas has 0 rows; quota visuals will show no real targets yet.")
                lines.extend(
                    [
                        "## Data Readiness Note",
                        "",
                        "`qc_system.project_state_quotas` has 0 rows. The report structure is ready, but quota visuals need real state targets.",
                        "",
                    ]
                )

    (POWERBI_DIR / "model_catalog.md").write_text("\n".join(lines), encoding="utf-8", newline="\n")
    if failures:
        for failure in failures:
            print(f"ERROR: {failure}")
        return 1
    print(f"Power BI catalog written to {POWERBI_DIR / 'model_catalog.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
