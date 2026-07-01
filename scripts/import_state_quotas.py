"""Import state-level quota targets from CSV into qc_system.project_state_quotas."""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path

import psycopg2


ROOT = Path(__file__).resolve().parents[1]


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


def load_registry() -> dict[str, dict]:
    path = ROOT / "config" / "surveycto_forms.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def parse_bool(value: str | None, default: bool = True) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "active"}


def clean_text(value: str | None) -> str:
    return " ".join((value or "").strip().split())


def import_quotas(csv_path: Path) -> int:
    registry = load_registry()
    load_env()
    rows: list[dict[str, object]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"form_id", "state_name", "quota_target"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required CSV column(s): {', '.join(sorted(missing))}")

        for row_number, raw in enumerate(reader, start=2):
            form_id = clean_text(raw.get("form_id"))
            state_name = clean_text(raw.get("state_name"))
            if not form_id or not state_name:
                raise ValueError(f"Row {row_number}: form_id and state_name are required.")
            try:
                quota_target = int(clean_text(raw.get("quota_target")))
            except ValueError as exc:
                raise ValueError(f"Row {row_number}: quota_target must be an integer.") from exc
            if quota_target < 0:
                raise ValueError(f"Row {row_number}: quota_target cannot be negative.")

            client_schema = clean_text(raw.get("client_schema"))
            if not client_schema:
                client_schema = registry.get(form_id, {}).get("schema", "")
            if not client_schema:
                raise ValueError(
                    f"Row {row_number}: client_schema is required because {form_id!r} is not in the registry."
                )

            rows.append(
                {
                    "form_id": form_id,
                    "client_schema": client_schema,
                    "state_name": state_name,
                    "quota_target": quota_target,
                    "wave_name": clean_text(raw.get("wave_name")) or "default",
                    "active": parse_bool(raw.get("active"), True),
                    "notes": clean_text(raw.get("notes")) or None,
                }
            )

    with psycopg2.connect(**db_kwargs()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists qc_system.project_state_quotas (
                    form_id       text not null,
                    client_schema text not null,
                    state_name    text not null,
                    quota_target  integer not null check (quota_target >= 0),
                    wave_name     text not null default 'default',
                    active        boolean not null default true,
                    notes         text,
                    created_at    timestamptz not null default now(),
                    updated_at    timestamptz not null default now(),
                    primary key (form_id, client_schema, state_name, wave_name)
                )
                """
            )
            for row in rows:
                cur.execute(
                    """
                    insert into qc_system.project_state_quotas (
                        form_id, client_schema, state_name, quota_target, wave_name, active, notes, updated_at
                    )
                    values (
                        %(form_id)s, %(client_schema)s, %(state_name)s, %(quota_target)s,
                        %(wave_name)s, %(active)s, %(notes)s, now()
                    )
                    on conflict (form_id, client_schema, state_name, wave_name) do update set
                        quota_target = excluded.quota_target,
                        active       = excluded.active,
                        notes        = excluded.notes,
                        updated_at   = now()
                    """,
                    row,
                )
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Import project/state quota targets from CSV.")
    parser.add_argument("csv_path", type=Path, help="CSV with form_id,state_name,quota_target columns.")
    args = parser.parse_args()
    count = import_quotas(args.csv_path)
    print(f"Imported {count} state quota row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
