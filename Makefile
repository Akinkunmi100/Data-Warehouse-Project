# ══════════════════════════════════════════
# Research Data Platform — Makefile
# ══════════════════════════════════════════
# Run from WSL2: make <target>

PROJECT_DIR := $(shell pwd)

.PHONY: up down restart test audit logs backup webhook deploy-flows install

# ── Service Lifecycle ──
up:
	docker compose up -d
	@echo "✅ Core services started"

down:
	docker compose down
	@echo "Services stopped"

restart:
	docker compose down && docker compose up -d
	@echo "✅ Services restarted"

# ── BI Services (on-demand) ──
metabase:
	docker compose --env-file secrets/.env -f docker-compose.bi.yml up metabase -d
	@echo "✅ Metabase started on http://localhost:3030"

superset:
	docker compose --env-file secrets/.env -f docker-compose.bi.yml up superset -d
	@echo "✅ Superset started on http://localhost:8088"

bi-stop:
	docker compose -f docker-compose.bi.yml stop
	@echo "BI services stopped"

# ── Testing & Auditing ──
test:
	python3 scripts/test_connectivity.py

audit:
	python3 scripts/deep_audit.py

# ── Operational Tasks ──
logs:
	docker compose logs --tail=50 -f

backup:
	bash scripts/backup_r2.sh

webhook:
	python3 -m uvicorn webhook.webhook_server:app --host 0.0.0.0 --port 8001

deploy-flows:
	python3 etl/pipelines/sync_surveycto.py &
	python3 etl/qc/qc_engine.py &
	@echo "✅ Prefect flows deployed in background"

# ── Setup ──
install:
	pip3 install --break-system-packages -r requirements.txt
	@echo "✅ Python dependencies installed"

db-shell:
	docker exec -it rp-postgres psql -U platform_admin -d warehouse

minio-shell:
	docker exec -it rp-minio sh
