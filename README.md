# Research Data Platform

A production-grade, single-machine data warehouse and ETL platform for survey research operations. Built with PostgreSQL 16, MinIO, Prefect 3, and FastAPI — running inside WSL2 (Ubuntu 24.04) + Docker Desktop.

---

## Architecture

```
SurveyCTO API  ──── (nightly cron) ────►  ETL Pipeline  ──► PostgreSQL Warehouse
                                                │
SurveyCTO Webhook ──► FastAPI Gateway ──► MinIO (raw-bronze)
                                                │
                                         Quality Control Engine
                                                │
                                         qc_system.qc_flags
                                                │
                                    Metabase / Superset (on-demand)
```

### Data Flow
1. **Webhook** — SurveyCTO pushes form submissions via HMAC-signed webhook to FastAPI → raw JSON archived in MinIO `raw-bronze` bucket.
2. **ETL** — Prefect orchestrates nightly incremental sync from the SurveyCTO V2 API → schema drift detection → flatten & upsert into PostgreSQL client schemas.
3. **QC** — Quality Control engine runs 5 automated checks per form: duplicate phones, speed violations, GPS boundaries, statistical outliers, and SurveyCTO review console rejections.
4. **BI** — Metabase or Superset connect to the warehouse for dashboards (started on-demand to save RAM).
5. **Backup** — Nightly pg_dump + MinIO archive uploaded to Cloudflare R2 with 30-day remote retention.

---

## Prerequisites

- Windows 10/11 with WSL2 enabled
- Ubuntu 24.04 WSL2 distro with user `platform`
- Docker Desktop (WSL2 integration enabled for Ubuntu-24.04)
- Python 3.12+ inside WSL2 (`pip install -r requirements.txt`)
- PostgreSQL client tools inside WSL2 (required by `backup_r2.sh`):
  ```bash
  sudo apt install postgresql-client
  ```
- AWS CLI inside WSL2 (required by `backup_r2.sh` for cloud upload):
  ```bash
  sudo apt install awscli
  ```

### WSL2 Memory Configuration (`C:\Users\<you>\.wslconfig`)
```ini
[wsl2]
memory=14GB
processors=4
swap=2GB
```

---

## Quick Start

### 1. Configure Credentials
```bash
# Edit secrets/.env with your real credentials:
nano secrets/.env
```
Fill in `SURVEYCTO_*` and `R2_*` values. All other defaults are ready to use.

### 2. Start Core Services
```bash
# From WSL2 terminal:
cd /mnt/c/Users/msi/Documents/Data\ Warehouse\ Project/research-platform
docker compose up -d
```

### 3. Verify Everything Works
```bash
# 40-check connectivity test
python3 scripts/test_connectivity.py

# 51-check deep quality audit
python3 scripts/deep_audit.py
```
Both scripts should report **0 failures** before proceeding.

### 4. Deploy Prefect Flows
```bash
# Starts nightly SurveyCTO sync cron at 01:00
python3 etl/pipelines/sync_surveycto.py

# Starts nightly QC engine cron at 01:30
python3 etl/qc/qc_engine.py
```

### 5. Start Webhook Receiver
```bash
python3 -m uvicorn webhook.webhook_server:app --host 0.0.0.0 --port 8001
```

### 6. Start BI Dashboard (On-Demand Only)
> ⚠️ Metabase and Superset each consume ~1.5 GB RAM. **Never start both at the same time.**
```bash
# Start Metabase only:
docker compose -f docker-compose.bi.yml up metabase -d

# OR start Superset only:
docker compose -f docker-compose.bi.yml up superset -d

# Stop BI services when done:
docker compose -f docker-compose.bi.yml stop
```

---

## Services & Ports

### Research Data Platform
| Service       | Host Port | URL                                  |
|---------------|-----------|--------------------------------------|
| PostgreSQL    | **5435**  | `localhost:5435`                     |
| MinIO API     | 9000      | `http://localhost:9000`              |
| MinIO Console | 9001      | `http://localhost:9001`              |
| Prefect UI    | 4200      | `http://localhost:4200`              |
| Webhook       | **8001**  | `http://localhost:8001`              |
| Metabase      | **3030**  | `http://localhost:3030` (on-demand)  |
| Superset      | 8088      | `http://localhost:8088` (on-demand)  |

> **Note:** PostgreSQL runs on port `5435`, the Webhook gateway runs on port `8001`, and Metabase runs on port `3030` (rather than standard defaults) to avoid conflicts with the NaijaFood services (occupying `5432`, `8000`, and `3000`). Both projects can run simultaneously without any port conflicts.

---

## Directory Structure

```
research-platform/
├── docker-compose.yml          # Core services (Postgres, MinIO, Prefect)
├── docker-compose.bi.yml       # On-demand BI (Metabase / Superset)
├── Makefile                    # Common operational shortcuts
├── requirements.txt            # Python dependencies
├── secrets/
│   └── .env                    # All credentials (never committed to Git)
├── scripts/
│   ├── init_db.sql             # Database schema + roles initialization
│   ├── backup_r2.sh            # Daily backup to Cloudflare R2
│   ├── test_connectivity.py    # Pre-flight connectivity test suite (40 checks)
│   └── deep_audit.py           # Deep quality & security audit (51 checks)
├── etl/
│   ├── pipelines/
│   │   └── sync_surveycto.py   # Incremental SurveyCTO ETL (Prefect flow)
│   └── qc/
│       └── qc_engine.py        # Quality control engine (Prefect flow)
├── webhook/
│   └── webhook_server.py       # FastAPI HMAC-authenticated webhook receiver
└── backup/                     # Local backup files (git-ignored)
```

---

## Database Schemas

| Schema            | Purpose                                | Access                    |
|-------------------|----------------------------------------|---------------------------|
| `client_mtn`      | MTN project survey data                | `etl_writer`, `analyst`   |
| `client_unilever` | Unilever project survey data           | `etl_writer`, `analyst`   |
| `internal`        | Internal research data                 | `etl_writer`, `analyst`   |
| `qc_system`       | QC flags, sync state, DLQ, audit logs  | `etl_writer` only         |

### Database Roles
| Role             | Purpose                                          |
|------------------|--------------------------------------------------|
| `platform_admin` | Superuser for migrations and admin tasks         |
| `etl_writer`     | Write access to all client schemas + qc_system   |
| `analyst_reader` | Read-only access to client schemas (no qc_system)|
| `etl_svc`        | Service account used by ETL containers           |
| `metabase_app`   | Metabase application database user               |

---

## Quality Control Engine

The QC engine runs 5 automated checks on every form dataset:

| Check | Severity | Description |
|---|---|---|
| Duplicate phone numbers | Critical | Same phone submitted on the same day |
| Speed violations | High | Duration below 10th percentile |
| GPS boundary violations | High | Coordinates outside configured regional bounds |
| Statistical outliers | Medium | Z-score > 3 on numeric fields |
| SurveyCTO rejections | High | Native review console `rejected` status |

All flags are stored in `qc_system.qc_flags` with JSONB detail payloads.

---

## SurveyCTO Webhook Integration

Endpoint: `POST /webhook/v1/{form_id}`

Authentication: HMAC-SHA256 signature via `X-SurveyCTO-Signature` header, or `?token=` query param fallback.

Supported form IDs (mapped in `webhook_server.py`):

| Form ID           | Client Schema     |
|-------------------|-------------------|
| `project_appraise`| `client_mtn`      |
| `unilever-retail` | `client_unilever` |
| `internal-census` | `internal`        |

---

## Backup Strategy

Daily automated backups via `scripts/backup_r2.sh` (cron: `0 2 * * *`):
- PostgreSQL `pg_dump` compressed with gzip
- MinIO data archive (tar.gz)
- **Local retention:** 7 days (always runs, even if cloud is not configured)
- **Remote retention:** 30 days (whichever cloud provider is active)
- **Growth alerting:** Logs a warning + audit entry if DB grows >10% day-over-day

### Cloud Backup — Tiered Strategy

The backup script tries cloud providers in order. Only one is used per run:

| Priority | Provider | Requirement | Notes |
|---|---|---|---|
| 1st | **Backblaze B2** | `B2_KEY_ID` + `B2_APPLICATION_KEY` set | Recommended — no credit card needed |
| 2nd | **Cloudflare R2** | `R2_ACCESS_KEY_ID` + `R2_SECRET_ACCESS_KEY` set | Used only if B2 is not configured |
| Fallback | **Local only** | *(neither set)* | Backups saved to `backup/` directory |

### Configuring Backblaze B2 (Recommended)
1. Sign up free at [backblaze.com](https://www.backblaze.com) — no credit card needed
2. Create a private bucket named `research-platform-backup`
3. Go to **App Keys** → **Add Application Key**, grant **Read & Write** access
4. Note the endpoint region from your bucket settings (e.g. `s3.us-west-004.backblazeb2.com`)

Fill in `secrets/.env`:
```env
B2_KEY_ID=your_key_id
B2_APPLICATION_KEY=your_application_key
B2_ENDPOINT=https://s3.us-west-004.backblazeb2.com
B2_BUCKET=research-platform-backup
```

### Configuring Cloudflare R2 (Optional Fallback)
Only needed if you have an existing Cloudflare account with a billing profile set up.

Fill in `secrets/.env`:
```env
R2_ACCESS_KEY_ID=your_r2_key
R2_SECRET_ACCESS_KEY=your_r2_secret
R2_ENDPOINT=https://ACCOUNT_ID.r2.cloudflarestorage.com
R2_BUCKET=research-platform-backup
```

---

## Security

- All credentials in `secrets/.env` (git-ignored, never committed)
- Webhook payload authentication via HMAC-SHA256 signatures
- Role-based PostgreSQL access (`etl_writer` / `analyst_reader` separation)
- `form_id` and `client_schema` SQL identifier sanitization via `_safe_id()` regex guard
- No `shell=True` subprocess calls anywhere
- `public` schema CREATE privilege revoked from the PUBLIC role

---

## Troubleshooting

### Docker Desktop not starting
```powershell
# Launch Docker Desktop manually:
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
# Wait ~60 seconds for the engine to initialize before running docker commands
```

### Port conflicts with other projects
The platform uses **non-default ports** to allow concurrent operation:
- PostgreSQL: `5435` (not 5432)
- All other services use unique ports (see table above)

### Prefect server crashing immediately
Ensure `docker-compose.yml` has `command: prefect server start` on the prefect service. Without it, the container exits after printing the logo.

### WSL systemd session error
The `wsl: Failed to start the systemd user session` error does not affect platform operation — it is a cosmetic WSL2 warning. Run scripts directly with:
```bash
wsl -d Ubuntu-24.04 -u platform -- bash -lc "cd '/mnt/c/.../research-platform' && python3 scripts/deep_audit.py"
```
