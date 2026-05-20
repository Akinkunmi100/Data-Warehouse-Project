#!/bin/bash
# ══════════════════════════════════════════
# Research Data Platform — Daily Backup & Cloudflare R2 Archival
# ══════════════════════════════════════════
# Set cron schedule: 0 4 * * * /path/to/scripts/backup_r2.sh >> /path/to/backup/backup.log 2>&1

set -euo pipefail

# Dynamically calculate absolute project directory path relative to script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BACKUP_DIR="${PROJECT_DIR}/backup"
SECRETS_FILE="${PROJECT_DIR}/secrets/.env"
LOG_FILE="${BACKUP_DIR}/backup.log"

# Setup directories
mkdir -p "${BACKUP_DIR}"
touch "${LOG_FILE}"

log() {
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $1" | tee -a "${LOG_FILE}"
}

log "🚀 Starting Daily Backup & Archival Routine..."

# Load credentials
if [ ! -f "${SECRETS_FILE}" ]; then
    log "❌ ERROR: Secrets file (.env) not found at ${SECRETS_FILE}!"
    exit 1
fi
set -o allexport
source "${SECRETS_FILE}"
set +o allexport

TIMESTAMP=$(date +'%Y%m%d_%H%M%S')
PG_DUMP_FILE="${BACKUP_DIR}/postgres_${TIMESTAMP}.sql.gz"
MINIO_TAR_FILE="${BACKUP_DIR}/minio_${TIMESTAMP}.tar.gz"

# 1. Postgres Database Backup
log "Executing PostgreSQL pg_dump..."
PGPASSWORD="${POSTGRES_PASSWORD}" pg_dump -h localhost -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" | gzip > "${PG_DUMP_FILE}"
log "✓ PostgreSQL pg_dump generated successfully: $(du -sh "${PG_DUMP_FILE}" | cut -f1)"

# 2. MinIO Object Data Backup
log "Creating MinIO storage archive..."
tar -czf "${MINIO_TAR_FILE}" -C "${PROJECT_DIR}" minio-data
log "✓ MinIO storage archive generated successfully: $(du -sh "${MINIO_TAR_FILE}" | cut -f1)"

# 3. Database Growth Monitoring Check (> 10% YoY/DoD)
log "Running database metrics growth checks..."
DB_SIZE_CURRENT=$(PGPASSWORD="${POSTGRES_PASSWORD}" psql -h localhost -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -t -A -c "SELECT pg_database_size('${POSTGRES_DB}')")

# Log database metrics metadata
SIZE_LOG="${BACKUP_DIR}/db_size_history.log"
if [ ! -f "${SIZE_LOG}" ]; then
    echo "date,size_bytes" > "${SIZE_LOG}"
fi

# Compare with yesterday's size if exists
YESTERDAY_BYTES=$(tail -n 1 "${SIZE_LOG}" | cut -d',' -f2 || echo "")
echo "$(date +'%Y-%m-%d'),${DB_SIZE_CURRENT}" >> "${SIZE_LOG}"

if [ -n "${YESTERDAY_BYTES}" ] && [ "${YESTERDAY_BYTES}" -ne 0 ] && [ "${YESTERDAY_BYTES}" != "size_bytes" ]; then
    GROWTH_PCT=$(awk "BEGIN {print (($DB_SIZE_CURRENT - $YESTERDAY_BYTES) / $YESTERDAY_BYTES) * 100}")
    log "Database current size: ${DB_SIZE_CURRENT} bytes. Growth day-over-day: ${GROWTH_PCT}%"
    
    # Alert if growth > 10%
    if (( $(echo "${GROWTH_PCT} > 10.0" | bc -l) )); then
        log "⚠️ WARNING: Database size is growing rapidly! Size increased by ${GROWTH_PCT}% in 24 hours."
        # Write to system audit logs
        PGPASSWORD="${POSTGRES_PASSWORD}" psql -h localhost -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c \
            "INSERT INTO qc_system.audit_log(action, detail) VALUES ('high_db_growth', '{\"growth_pct\": ${GROWTH_PCT}, \"current_size_bytes\": ${DB_SIZE_CURRENT}}'::jsonb)" > /dev/null || true
    fi
fi

# 4. Upload Backups to Cloudflare R2 via AWS CLI (S3 Compatibility API)
if [ -z "${R2_ACCESS_KEY_ID:-}" ] || [ -z "${R2_SECRET_ACCESS_KEY:-}" ] || [ -z "${R2_ENDPOINT:-}" ]; then
    log "⚠️ Skipping Cloudflare R2 upload: R2 credentials are not filled in .env yet."
else
    log "Configuring temporary S3 credentials for Cloudflare R2..."
    export AWS_ACCESS_KEY_ID="${R2_ACCESS_KEY_ID}"
    export AWS_SECRET_ACCESS_KEY="${R2_SECRET_ACCESS_KEY}"
    
    log "Uploading Postgres backup to R2..."
    aws s3 cp "${PG_DUMP_FILE}" "s3://${R2_BUCKET}/db/postgres_${TIMESTAMP}.sql.gz" --endpoint-url "${R2_ENDPOINT}"
    
    log "Uploading MinIO backup to R2..."
    aws s3 cp "${MINIO_TAR_FILE}" "s3://${R2_BUCKET}/storage/minio_${TIMESTAMP}.tar.gz" --endpoint-url "${R2_ENDPOINT}"
    
    log "✓ Cloud uploads finished."
    
    # 5. Remote R2 retention rules check (Prune remote objects older than 30 days)
    log "Executing remote backup pruning (30 days retention)..."
    LIMIT_DATE=$(date -d "30 days ago" +%s)
    
    aws s3 ls "s3://${R2_BUCKET}/db/" --endpoint-url "${R2_ENDPOINT}" | while read -r line; do
        FILE_DATE=$(echo "$line" | awk '{print $1" "$2}')
        FILE_NAME=$(echo "$line" | awk '{print $4}')
        FILE_EPOCH=$(date -d "$FILE_DATE" +%s)
        
        if [ "${FILE_EPOCH}" -lt "${LIMIT_DATE}" ]; then
            log "Pruning remote file: s3://${R2_BUCKET}/db/${FILE_NAME}"
            aws s3 rm "s3://${R2_BUCKET}/db/${FILE_NAME}" --endpoint-url "${R2_ENDPOINT}"
        fi
    done
fi

# 6. Local Rotation Check (Prune files older than 7 days)
log "Running local backup pruning (7 days retention)..."
find "${BACKUP_DIR}" -type f -name "postgres_*.sql.gz" -mtime +7 -delete
find "${BACKUP_DIR}" -type f -name "minio_*.tar.gz" -mtime +7 -delete
log "✓ Local rotation complete."

log "✨ Daily Backup & Archival process finished successfully."
