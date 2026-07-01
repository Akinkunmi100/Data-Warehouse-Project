# ══════════════════════════════════════════
# Research Data Platform — Makefile
# ══════════════════════════════════════════
# Run from WSL2: make <target>

PROJECT_DIR := $(shell pwd)

.PHONY: up down restart test audit logs backup webhook deploy-flows install onboard \
        setup-buckets file-watcher dbt-build dbt-test dbt-docs \
        sync-forms dashboards powerbi-assets metabase superset bi-stop db-shell minio-shell

# ══════════════════════════════════════════
# SERVICE LIFECYCLE
# ══════════════════════════════════════════

up:
	docker compose up -d
	@echo "✅ Core services started (postgres, minio, prefect)"

down:
	docker compose down
	@echo "Services stopped"

restart:
	docker compose down && docker compose up -d
	@echo "✅ Services restarted"

# ── BI Services (on-demand, start separately to save RAM) ────────────────────
metabase:
	docker compose --env-file secrets/.env -f docker-compose.bi.yml up metabase -d
	@echo "✅ Metabase started → http://localhost:3030"

superset:
	docker compose --env-file secrets/.env -f docker-compose.bi.yml up superset -d
	@echo "✅ Superset started → http://localhost:8088"

bi-stop:
	docker compose --env-file secrets/.env -f docker-compose.bi.yml stop
	@echo "BI services stopped"

# ══════════════════════════════════════════
# FIRST-TIME SETUP
# ══════════════════════════════════════════

install:
	pip3 install --break-system-packages -r requirements.txt
	@echo "✅ Python dependencies installed"
	@echo "   Running dbt deps to download dbt packages (dbt_utils etc.) ..."
	cd dbt/research_platform && dbt deps
	@echo "✅ dbt packages installed"

onboard:
	@if [ -z "$(FORM)" ]; then echo "Usage: make onboard FORM=<surveycto_form_id>"; exit 1; fi
	python3 scripts/onboard_surveycto_form.py $(FORM)

# Creates all four required MinIO buckets if they don't already exist.
# Idempotent — safe to run multiple times.
setup-buckets:
	@echo "Creating MinIO buckets..."
	@python3 -c "\
import boto3, os; \
from botocore.client import Config; \
from dotenv import load_dotenv; \
load_dotenv('secrets/.env'); \
s3 = boto3.client('s3', \
    endpoint_url='http://localhost:9000', \
    aws_access_key_id=os.getenv('MINIO_ROOT_USER'), \
    aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'), \
    config=Config(signature_version='s3v4')); \
existing = [b['Name'] for b in s3.list_buckets()['Buckets']]; \
needed   = ['raw-bronze', 'processed-silver', 'exports', 'backup-staging']; \
[s3.create_bucket(Bucket=b) or print(f'  Created: {b}') for b in needed if b not in existing]; \
[print(f'  Already exists: {b}') for b in needed if b in existing]; \
print('✅ All MinIO buckets ready')"

# ══════════════════════════════════════════
# RUNTIME WORKERS
# ══════════════════════════════════════════

webhook:
	python3 -m uvicorn webhook.webhook_server:app --host 0.0.0.0 --port 8001 --reload
	@echo "✅ Webhook receiver started on :8001"

# Registers all three form ETL + QC deployments with Prefect and starts serving them.
deploy-flows:
	@echo "Deploying ETL and QC flows to Prefect..."
	python3 etl/pipelines/sync_surveycto.py &
	python3 etl/qc/qc_engine.py &
	@echo "✅ ETL + QC flows deployed (running in background)"

# Watches uploads/ for CSV/TSV/XLSX/XLS/SAV files and triggers the ETL pipeline.
# Run this alongside webhook and deploy-flows for a fully automated ingest path.
file-watcher:
	@echo "Starting file watcher on uploads/ ..."
	python3 etl/pipelines/file_watcher.py

# ══════════════════════════════════════════
# DBT OPERATIONS
# ══════════════════════════════════════════

dbt-build:
	@echo "Running dbt build (compile + run + test all models) ..."
	cd dbt/research_platform && dbt build
	@echo "✅ dbt build complete"

dbt-test:
	@echo "Running dbt tests ..."
	cd dbt/research_platform && dbt test
	@echo "✅ dbt tests complete"

dbt-docs:
	@echo "Generating and serving dbt documentation ..."
	cd dbt/research_platform && dbt docs generate && dbt docs serve --port 8080

sync-forms:
	python3 scripts/sync_registered_forms.py
	@echo "Registered forms synced into qc_system.registered_forms"

dashboards: sync-forms dbt-build
	python3 scripts/rebuild_metabase_dashboards.py
	@echo "Metabase dashboards rebuilt"

powerbi-assets: dbt-build
	python3 scripts/validate_powerbi_model.py
	@echo "Power BI model contract validated; open powerbi/README.md for build steps"

# ══════════════════════════════════════════
# OPERATIONAL TASKS
# ══════════════════════════════════════════

test:
	python3 scripts/test_connectivity.py

audit:
	python3 scripts/deep_audit.py

logs:
	docker compose logs --tail=50 -f

backup:
	bash scripts/backup_r2.sh

# ══════════════════════════════════════════
# SHELLS (debug access)
# ══════════════════════════════════════════

db-shell:
	docker exec -it rp-postgres psql -U platform_admin -d warehouse

minio-shell:
	docker exec -it rp-minio sh
