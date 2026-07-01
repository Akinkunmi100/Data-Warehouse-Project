"""SurveyCTO form registry utilities.

The registry is the single source of truth for active SurveyCTO forms. Add a
form once with scripts/register_surveycto_form.py and the test suite, webhook,
ETL deployments, and QC deployments will all pick it up.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from etl.utils import safe_id


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_PATH = PROJECT_ROOT / "config" / "surveycto_forms.json"


DEFAULT_REGISTRY: dict[str, dict[str, Any]] = {
    "project_appraise": {
        "active": True,
        "client": "client_mtn",
        "schema": "client_mtn",
        "etl_deployment": "surveycto-nightly-mtn",
        "qc_deployment": "qc-engine-nightly-mtn",
        "etl_cron": "0 1 * * *",
        "qc_cron": "30 1 * * *",
    }
}


def registry_path() -> Path:
    """Return the configured registry path."""
    override = os.getenv("SURVEYCTO_REGISTRY_PATH")
    return Path(override).resolve() if override else DEFAULT_REGISTRY_PATH


def form_slug(form_id: str) -> str:
    """Return a deployment-safe slug."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", form_id.strip().lower()).strip("-")
    return slug or "surveycto-form"


def default_schema_for_form(form_id: str) -> str:
    """Derive a warehouse schema from a form ID."""
    return "client_" + safe_id(form_id.strip().lower()).strip("_")


def normalize_form_config(form_id: str, config: dict[str, Any] | str) -> dict[str, Any]:
    """Fill derived registry fields for one form."""
    if isinstance(config, str):
        config = {"client": config, "schema": config}
    else:
        config = dict(config)

    schema = config.get("schema") or config.get("client") or default_schema_for_form(form_id)
    client = config.get("client") or schema
    slug = form_slug(form_id)

    config["active"] = bool(config.get("active", True))
    config["client"] = safe_id(str(client))
    config["schema"] = safe_id(str(schema))
    config.setdefault("etl_deployment", f"surveycto-nightly-{slug}")
    config.setdefault("qc_deployment", f"qc-engine-nightly-{slug}")
    config.setdefault("etl_cron", "0 1 * * *")
    config.setdefault("qc_cron", "30 1 * * *")
    return config


def load_form_registry(active_only: bool = True) -> dict[str, dict[str, Any]]:
    """Load and normalize the SurveyCTO form registry."""
    path = registry_path()
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    else:
        raw = DEFAULT_REGISTRY

    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain a JSON object keyed by form ID")

    registry = {
        str(form_id): normalize_form_config(str(form_id), config)
        for form_id, config in raw.items()
    }
    if active_only:
        registry = {
            form_id: config
            for form_id, config in registry.items()
            if config.get("active", True)
        }
    return registry


def save_form_registry(registry: dict[str, dict[str, Any]]) -> None:
    """Persist the form registry."""
    path = registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = {
        form_id: normalize_form_config(form_id, config)
        for form_id, config in sorted(registry.items())
    }
    with path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(normalized, f, indent=2)
        f.write("\n")


def get_form_config(form_id: str, active_only: bool = True) -> dict[str, Any] | None:
    """Return one form registry entry, or None if it is unknown."""
    return load_form_registry(active_only=active_only).get(form_id)


def register_form(
    form_id: str,
    client: str | None = None,
    schema: str | None = None,
    active: bool = True,
) -> dict[str, Any]:
    """Add or update one form in the registry and return its config."""
    clean_form_id = form_id.strip()
    if not clean_form_id:
        raise ValueError("form_id is required")

    registry = load_form_registry(active_only=False)
    existing = registry.get(clean_form_id, {})
    config = {
        **existing,
        "active": active,
        "client": client or existing.get("client") or schema or default_schema_for_form(clean_form_id),
        "schema": schema or existing.get("schema") or client or default_schema_for_form(clean_form_id),
    }
    config = normalize_form_config(clean_form_id, config)
    registry[clean_form_id] = config
    save_form_registry(registry)
    return config
