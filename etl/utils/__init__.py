# ══════════════════════════════════════════
# Research Data Platform — ETL Shared Utilities
# ══════════════════════════════════════════

import re
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine as _sa_create_engine


def safe_id(name: str) -> str:
    """Sanitize a SQL identifier — strip everything except alphanumerics and underscores."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)


def build_db_url() -> str:
    """Return a URL-encoded SQLAlchemy connection string from environment variables.

    Password is encoded with urllib.parse.quote_plus so that special characters
    (@, #, ?, /, etc.) in POSTGRES_PASSWORD do not corrupt the URL parser and
    cause a silent authentication failure.

    This is the single source of truth imported by webhook_server.py,
    sync_surveycto.py, and qc_engine.py.
    """
    user     = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD", "")
    db       = os.getenv("POSTGRES_DB")
    host     = os.getenv("POSTGRES_HOST", "localhost")
    port     = os.getenv("POSTGRES_PORT", "5435")
    return f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{db}"


def get_db_engine():
    """Build and return a SQLAlchemy engine from environment variables.

    Delegates URL construction to build_db_url() so password encoding
    is consistent across all callers.
    """
    return _sa_create_engine(build_db_url())
