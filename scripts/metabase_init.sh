#!/bin/bash
# ══════════════════════════════════════════
# Research Data Platform — Metabase Database Initialiser
# Called by the metabase-db-init container in docker-compose.yml
# ══════════════════════════════════════════
set -e

log() { echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $1"; }

for var in POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB METABASE_DB_NAME METABASE_DB_USER METABASE_DB_PASSWORD; do
    if [ -z "${!var:-}" ]; then
        log "ERROR: Required variable $var is not set. Aborting Metabase DB init."
        exit 1
    fi
done

PGPASSWORD="${POSTGRES_PASSWORD}"
export PGPASSWORD

# FIX: replaced `sleep 8` with a pg_isready polling loop.
# On a cold start with an uninitialised NTFS volume, PostgreSQL can take well
# over 8 seconds to accept connections. A fixed sleep is a race condition that
# fails silently and leaves Metabase unable to start.
# We poll every 2 seconds for up to 60 seconds, then give up cleanly.
log "Waiting for PostgreSQL to be ready (polling pg_isready, up to 60 s)..."
MAX_ATTEMPTS=30
ATTEMPT=0
until pg_isready -h postgres -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -q; do
    ATTEMPT=$((ATTEMPT + 1))
    if [ "${ATTEMPT}" -ge "${MAX_ATTEMPTS}" ]; then
        log "ERROR: PostgreSQL did not become ready within 60 seconds. Aborting."
        exit 1
    fi
    sleep 2
done
log "PostgreSQL is ready after $((ATTEMPT * 2)) seconds."

run_sql() {
    psql -h postgres -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "$1" 2>&1
}

# Create database if it doesn't already exist
log "Creating Metabase database '${METABASE_DB_NAME}' if not exists..."
DB_EXISTS=$(psql -h postgres -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -tAc \
    "SELECT 1 FROM pg_database WHERE datname='${METABASE_DB_NAME}'" 2>&1)

if [ "$DB_EXISTS" = "1" ]; then
    log "Database '${METABASE_DB_NAME}' already exists — skipping CREATE."
else
    run_sql "CREATE DATABASE ${METABASE_DB_NAME};"
    log "Database '${METABASE_DB_NAME}' created."
fi

# Create user if not exists
log "Ensuring Metabase DB user '${METABASE_DB_USER}' exists..."
USER_EXISTS=$(psql -h postgres -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -tAc \
    "SELECT 1 FROM pg_roles WHERE rolname='${METABASE_DB_USER}'" 2>&1)

if [ "$USER_EXISTS" = "1" ]; then
    run_sql "ALTER USER ${METABASE_DB_USER} WITH PASSWORD '${METABASE_DB_PASSWORD}';"
    log "User '${METABASE_DB_USER}' password updated."
else
    run_sql "CREATE USER ${METABASE_DB_USER} WITH PASSWORD '${METABASE_DB_PASSWORD}';"
    log "User '${METABASE_DB_USER}' created."
fi

run_sql "ALTER DATABASE ${METABASE_DB_NAME} OWNER TO ${METABASE_DB_USER};"
log "Ownership of '${METABASE_DB_NAME}' set to '${METABASE_DB_USER}'."

log "Metabase database initialisation complete."
