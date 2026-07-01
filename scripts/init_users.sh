#!/bin/sh
# ══════════════════════════════════════════
# Research Data Platform — Service Account Password Initialiser
# Runs as /docker-entrypoint-initdb.d/02_init_users.sh AFTER 01_init.sql
# Required env vars (from secrets/.env via docker-compose env_file):
#   ETL_SVC_PASSWORD, ANALYST_PASSWORD, METABASE_APP_PASSWORD
# ══════════════════════════════════════════
set -e

log() { echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $1"; }

# Validate required env vars
for var in ETL_SVC_PASSWORD ANALYST_PASSWORD METABASE_APP_PASSWORD; do
    eval "value=\${$var:-}"
    if [ -z "$value" ]; then
        log "ERROR: $var is not set in secrets/.env — aborting to prevent empty passwords."
        exit 1
    fi
done

log "Applying service account passwords from environment variables..."

# Use psql -v to pass passwords as safe bind variables (handles quoting correctly)
psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname   "$POSTGRES_DB"   \
     -v "etl_pw=$ETL_SVC_PASSWORD"          \
     -v "analyst_pw=$ANALYST_PASSWORD"       \
     -v "metabase_pw=$METABASE_APP_PASSWORD" \
     << 'EOSQL'
ALTER ROLE etl_svc      WITH LOGIN PASSWORD :'etl_pw';
ALTER ROLE analyst       WITH LOGIN PASSWORD :'analyst_pw';
ALTER ROLE metabase_app  WITH LOGIN PASSWORD :'metabase_pw';
\echo 'All service account passwords updated successfully.'
EOSQL

log "Password initialisation complete."
