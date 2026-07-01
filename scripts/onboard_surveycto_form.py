#!/usr/bin/env python3
"""Onboard one SurveyCTO form end to end.

Usage:
    python scripts/onboard_surveycto_form.py my_form_id

The command verifies access, registers the form, creates warehouse grants,
regenerates dbt sources/models, runs the initial ETL, builds dbt, and rebuilds
Metabase dashboards.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_env() -> dict[str, str]:
    env = os.environ.copy()
    env_path = ROOT / "secrets" / ".env"
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def run_step(label: str, command: list[str], env: dict[str, str], cwd: Path | None = None) -> None:
    print(f"\n==> {label}")
    print("    " + " ".join(command))
    result = subprocess.run(command, cwd=cwd or ROOT, env=env)
    if result.returncode != 0:
        raise SystemExit(f"\nStep failed: {label} (exit {result.returncode})")


def main() -> int:
    parser = argparse.ArgumentParser(description="Onboard a SurveyCTO form and rebuild dashboards")
    parser.add_argument("form_id", help="Exact SurveyCTO form ID")
    parser.add_argument("--client", help="Optional logical client name")
    parser.add_argument("--schema", help="Optional warehouse schema. Defaults to client_<form_id>.")
    parser.add_argument("--inactive", action="store_true", help="Register but do not schedule/run ETL")
    parser.add_argument("--skip-probe", action="store_true", help="Skip SurveyCTO access probe during registration")
    parser.add_argument("--skip-etl", action="store_true", help="Do not run the initial ETL")
    parser.add_argument("--skip-dbt", action="store_true", help="Do not run dbt build")
    parser.add_argument("--skip-dashboards", action="store_true", help="Do not rebuild Metabase dashboards")
    args = parser.parse_args()

    env = load_env()
    python = sys.executable

    register_cmd = [python, "scripts/register_surveycto_form.py", args.form_id]
    if args.client:
        register_cmd.extend(["--client", args.client])
    if args.schema:
        register_cmd.extend(["--schema", args.schema])
    if args.inactive:
        register_cmd.append("--inactive")
    if args.skip_probe:
        register_cmd.append("--skip-probe")
    run_step("Register form and warehouse schema", register_cmd, env)

    run_step(
        "Sync registry into warehouse and generate dbt source/model files",
        [python, "scripts/sync_registered_forms.py"],
        env,
    )

    if not args.inactive:
        run_step(
            "Verify SurveyCTO fetch access",
            [python, "scripts/test_surveycto.py", "--form", args.form_id, "--fetch-only"],
            env,
        )

    if not args.inactive and not args.skip_etl:
        run_step(
            "Run initial ETL and QC",
            [python, "etl/pipelines/sync_surveycto.py", args.form_id],
            env,
        )

    if not args.skip_dbt:
        run_step(
            "Build dbt analytical layer",
            ["dbt", "build", "--no-version-check"],
            {**env, "DBT_PROFILES_DIR": str(ROOT / "dbt" / "research_platform")},
            cwd=ROOT / "dbt" / "research_platform",
        )

    if not args.skip_dashboards:
        run_step(
            "Rebuild Metabase dashboards",
            [python, "scripts/rebuild_metabase_dashboards.py"],
            env,
        )

    print("\nOnboarding complete.")
    print("Metabase: http://localhost:3030")
    print("Prefect:  http://localhost:4200")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
