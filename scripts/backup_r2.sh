#!/bin/bash
# ══════════════════════════════════════════
# Research Data Platform — Daily Backup & Cloud Archival
# ══════════════════════════════════════════
# Cron: 0 2 * * * /path/to/scripts/backup_r2.sh >> /path/to/backup/backup.log 2>&1
#
# Schedule: 2am daily. ETL nightly poll runs at 1am — 1 hour gap avoids
# a backup starting mid-write. Do NOT change this to 1am or 4am.
#
# FIX (was broken): MinIO data lives in Docker named volume 'rp-minio-data',
# NOT in ./minio-data/ on disk. We now export it via a temporary Alpine container.

set -euo pipefail

export PATH="/home/platform/.local/bin:/usr/local/bin:/usr/bin:/bin:${PATH:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BACKUP_DIR="${PROJECT_DIR}/backup"
SECRETS_FILE="${PROJECT_DIR}/secrets/.env"
LOG_FILE="${BACKUP_DIR}/backup.log"

mkdir -p "${BACKUP_DIR}"
touch "${LOG_FILE}"

log() {
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $1" | tee -a "${LOG_FILE}"
}

log "🚀 Starting Daily Backup & Archival Routine..."

# Load credentials
if [ ! -f "${SECRETS_FILE}" ]; then
    log "❌ ERROR: Secrets file not found at ${SECRETS_FILE}!"
    exit 1
fi
set -o allexport
source "${SECRETS_FILE}"
set +o allexport

TIMESTAMP=$(date +'%Y%m%d_%H%M%S')
PG_DUMP_FILE="${BACKUP_DIR}/postgres_${TIMESTAMP}.sql.gz"
METABASE_DUMP_FILE="${BACKUP_DIR}/metabaseappdb_${TIMESTAMP}.sql.gz"
MINIO_TAR_FILE="${BACKUP_DIR}/minio_${TIMESTAMP}.tar.gz"

# ── 1. PostgreSQL Backup (pg_dump — correct approach for named volumes) ──────
log "Executing PostgreSQL pg_dump for warehouse..."
PGPASSWORD="${POSTGRES_PASSWORD}" pg_dump \
    -h localhost -p 5435 \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" | gzip > "${PG_DUMP_FILE}"
log "✓ pg_dump warehouse complete: $(du -sh "${PG_DUMP_FILE}" | cut -f1)"

log "Executing PostgreSQL pg_dump for metabaseappdb..."
PGPASSWORD="${POSTGRES_PASSWORD}" pg_dump \
    -h localhost -p 5435 \
    -U "${POSTGRES_USER}" \
    -d "${METABASE_DB_NAME:-metabaseappdb}" | gzip > "${METABASE_DUMP_FILE}"
log "✓ pg_dump metabaseappdb complete: $(du -sh "${METABASE_DUMP_FILE}" | cut -f1)"

# ── 2. MinIO Backup via Docker volume export ──────────────────────────────────
# IMPORTANT: MinIO uses a Docker NAMED VOLUME (rp-minio-data), not a bind mount.
# ./minio-data/ on disk is empty. We must export via a container that mounts the volume.
log "Exporting MinIO data from Docker volume 'rp-minio-data'..."
if docker volume inspect rp-minio-data >/dev/null 2>&1; then
    docker run --rm \
        -v rp-minio-data:/minio-data:ro \
        alpine \
        tar -czf - -C /minio-data . > "${MINIO_TAR_FILE}"
    log "✓ MinIO volume export complete: $(du -sh "${MINIO_TAR_FILE}" | cut -f1)"
else
    log "⚠️  Docker volume 'rp-minio-data' not found — MinIO may not have started yet. Skipping."
fi

# ── 3. Database Growth Monitoring ─────────────────────────────────────────────
log "Running database growth check..."
DB_SIZE_CURRENT=$(PGPASSWORD="${POSTGRES_PASSWORD}" psql \
    -h localhost -p 5435 \
    -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
    -t -A -c "SELECT pg_database_size('${POSTGRES_DB}')")

SIZE_LOG="${BACKUP_DIR}/db_size_history.log"
if [ ! -f "${SIZE_LOG}" ]; then
    echo "date,size_bytes" > "${SIZE_LOG}"
fi

YESTERDAY_BYTES=$(tail -n 1 "${SIZE_LOG}" 2>/dev/null | cut -d',' -f2 || echo "")
echo "$(date +'%Y-%m-%d'),${DB_SIZE_CURRENT}" >> "${SIZE_LOG}"

if [[ -n "${YESTERDAY_BYTES}" && "${YESTERDAY_BYTES}" =~ ^[0-9]+$ && "${YESTERDAY_BYTES}" -ne 0 ]]; then
    GROWTH_PCT=$(awk "BEGIN {print (($DB_SIZE_CURRENT - $YESTERDAY_BYTES) / $YESTERDAY_BYTES) * 100}")
    log "DB size: ${DB_SIZE_CURRENT} bytes. Day-over-day growth: ${GROWTH_PCT}%"

    if (( $(echo "${GROWTH_PCT} > 10.0" | bc -l) )); then
        log "⚠️  WARNING: DB grew ${GROWTH_PCT}% in 24 hours!"
        PGPASSWORD="${POSTGRES_PASSWORD}" psql \
            -h localhost -p 5435 \
            -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c \
            "INSERT INTO qc_system.audit_log(action, detail)
             VALUES ('high_db_growth', '{\"growth_pct\": ${GROWTH_PCT}, \"current_size_bytes\": ${DB_SIZE_CURRENT}}'::jsonb)" > /dev/null || true
    fi
fi

# ── 4. Cloud Upload (B2 primary → R2 fallback → local-only) ──────────────────
CLOUD_UPLOADED=false

if [ -n "${B2_KEY_ID:-}" ] && [ -n "${B2_APPLICATION_KEY:-}" ] && [ -n "${B2_ENDPOINT:-}" ]; then
    log "☁️  Cloud provider: Backblaze B2 (primary)"
    export AWS_ACCESS_KEY_ID="${B2_KEY_ID}"
    export AWS_SECRET_ACCESS_KEY="${B2_APPLICATION_KEY}"
    BUCKET="${B2_BUCKET}"
    ENDPOINT="${B2_ENDPOINT}"

    aws s3 cp "${PG_DUMP_FILE}"   "s3://${BUCKET}/db/postgres_${TIMESTAMP}.sql.gz"    --endpoint-url "${ENDPOINT}" && log "✓ Postgres (warehouse) → B2"
    aws s3 cp "${METABASE_DUMP_FILE}" "s3://${BUCKET}/db/metabaseappdb_${TIMESTAMP}.sql.gz" --endpoint-url "${ENDPOINT}" && log "✓ Postgres (metabase) → B2"
    [ -f "${MINIO_TAR_FILE}" ] && \
    aws s3 cp "${MINIO_TAR_FILE}" "s3://${BUCKET}/storage/minio_${TIMESTAMP}.tar.gz"  --endpoint-url "${ENDPOINT}" && log "✓ MinIO   → B2"

    log "Pruning B2 objects older than 30 days..."
    LIMIT_DATE=$(date -d "30 days ago" +%s)
    for prefix in db storage; do
        aws s3 ls "s3://${BUCKET}/${prefix}/" --endpoint-url "${ENDPOINT}" | while read -r line; do
            FILE_DATE=$(echo "$line" | awk '{print $1" "$2}')
            FILE_NAME=$(echo "$line" | awk '{print $4}')
            FILE_EPOCH=$(date -d "$FILE_DATE" +%s)
            if [ "${FILE_EPOCH}" -lt "${LIMIT_DATE}" ]; then
                log "  Pruning: s3://${BUCKET}/${prefix}/${FILE_NAME}"
                aws s3 rm "s3://${BUCKET}/${prefix}/${FILE_NAME}" --endpoint-url "${ENDPOINT}"
            fi
        done
    done
    CLOUD_UPLOADED=true

elif [ -n "${R2_ACCESS_KEY_ID:-}" ] && [ -n "${R2_SECRET_ACCESS_KEY:-}" ] && [ -n "${R2_ENDPOINT:-}" ]; then
    log "☁️  Cloud provider: Cloudflare R2 (fallback)"
    export AWS_ACCESS_KEY_ID="${R2_ACCESS_KEY_ID}"
    export AWS_SECRET_ACCESS_KEY="${R2_SECRET_ACCESS_KEY}"
    BUCKET="${R2_BUCKET}"
    ENDPOINT="${R2_ENDPOINT}"

    aws s3 cp "${PG_DUMP_FILE}"   "s3://${BUCKET}/db/postgres_${TIMESTAMP}.sql.gz"    --endpoint-url "${ENDPOINT}" && log "✓ Postgres (warehouse) → R2"
    aws s3 cp "${METABASE_DUMP_FILE}" "s3://${BUCKET}/db/metabaseappdb_${TIMESTAMP}.sql.gz" --endpoint-url "${ENDPOINT}" && log "✓ Postgres (metabase) → R2"
    [ -f "${MINIO_TAR_FILE}" ] && \
    aws s3 cp "${MINIO_TAR_FILE}" "s3://${BUCKET}/storage/minio_${TIMESTAMP}.tar.gz"  --endpoint-url "${ENDPOINT}" && log "✓ MinIO   → R2"

    log "Pruning R2 objects older than 30 days..."
    LIMIT_DATE=$(date -d "30 days ago" +%s)
    for prefix in db storage; do
        aws s3 ls "s3://${BUCKET}/${prefix}/" --endpoint-url "${ENDPOINT}" | while read -r line; do
            FILE_DATE=$(echo "$line" | awk '{print $1" "$2}')
            FILE_NAME=$(echo "$line" | awk '{print $4}')
            FILE_EPOCH=$(date -d "$FILE_DATE" +%s)
            if [ "${FILE_EPOCH}" -lt "${LIMIT_DATE}" ]; then
                log "  Pruning: s3://${BUCKET}/${prefix}/${FILE_NAME}"
                aws s3 rm "s3://${BUCKET}/${prefix}/${FILE_NAME}" --endpoint-url "${ENDPOINT}"
            fi
        done
    done
    CLOUD_UPLOADED=true

else
    log "⚠️  No cloud credentials configured — local backup only."
    log "    Set B2_KEY_ID + B2_APPLICATION_KEY (Backblaze B2) or"
    log "    R2_ACCESS_KEY_ID + R2_SECRET_ACCESS_KEY (Cloudflare R2) in secrets/.env"
fi

[ "${CLOUD_UPLOADED}" = true ] && log "☁️  Remote backup upload complete."

# ── 5. Local Retention (7 days) ───────────────────────────────────────────────
log "Pruning local backups older than 7 days..."
find "${BACKUP_DIR}" -type f -name "postgres_*.sql.gz"  -mtime +7 -delete
find "${BACKUP_DIR}" -type f -name "metabaseappdb_*.sql.gz" -mtime +7 -delete
find "${BACKUP_DIR}" -type f -name "minio_*.tar.gz"     -mtime +7 -delete
log "✓ Local rotation complete."

log "✨ Daily Backup & Archival finished successfully."
