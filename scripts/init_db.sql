-- ══════════════════════════════════════════
-- Research Data Platform — Initial Database Setup
-- ══════════════════════════════════════════
-- NOTE: User/role passwords are NOT set here.
-- They are applied by 02_init_users.sh which reads from secrets/.env.
-- Required .env keys: ETL_SVC_PASSWORD, ANALYST_PASSWORD, METABASE_APP_PASSWORD

-- ── Warehouse schemas (client data isolation) ─────────────────────────────
CREATE SCHEMA IF NOT EXISTS client_mtn;
CREATE SCHEMA IF NOT EXISTS client_unilever;
CREATE SCHEMA IF NOT EXISTS internal;
CREATE SCHEMA IF NOT EXISTS qc_system;

-- ── Medallion schemas (created here so grants are applied before dbt runs) ─
-- dbt's generate_schema_name macro uses the custom schema name verbatim,
-- so these schemas must be named bronze/silver/gold, not public_bronze etc.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ── Roles (login credentials applied separately by 02_init_users.sh) ──────
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_writer') THEN
        CREATE ROLE etl_writer;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_reader') THEN
        CREATE ROLE analyst_reader;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_svc') THEN
        CREATE USER etl_svc WITH PASSWORD 'PENDING_SET_BY_INIT_SCRIPT';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst') THEN
        CREATE USER analyst WITH PASSWORD 'PENDING_SET_BY_INIT_SCRIPT';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'metabase_app') THEN
        CREATE USER metabase_app WITH PASSWORD 'PENDING_SET_BY_INIT_SCRIPT';
    END IF;
END
$$;

-- ── Schema usage grants ────────────────────────────────────────────────────
GRANT USAGE ON SCHEMA client_mtn, client_unilever, internal, qc_system TO etl_writer;

-- Analyst and Metabase can read warehouse + all medallion schemas
GRANT USAGE ON SCHEMA client_mtn, client_unilever, internal TO analyst_reader;
GRANT USAGE ON SCHEMA client_mtn, client_unilever, internal TO metabase_app;
GRANT USAGE ON SCHEMA bronze, silver, gold TO analyst_reader;
GRANT USAGE ON SCHEMA bronze, silver, gold TO metabase_app;

-- ── Bind service users to roles ───────────────────────────────────────────
GRANT etl_writer   TO etl_svc;
GRANT analyst_reader TO analyst;
GRANT analyst_reader TO metabase_app;

-- ── Default privileges for tables created in the FUTURE ───────────────────
-- Warehouse schemas (ETL write + analyst/metabase read)
ALTER DEFAULT PRIVILEGES IN SCHEMA client_mtn      GRANT INSERT, UPDATE, SELECT, DELETE ON TABLES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA client_unilever GRANT INSERT, UPDATE, SELECT, DELETE ON TABLES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA internal        GRANT INSERT, UPDATE, SELECT, DELETE ON TABLES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA qc_system       GRANT INSERT, UPDATE, SELECT, DELETE ON TABLES TO etl_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA client_mtn      GRANT SELECT ON TABLES TO analyst_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA client_unilever GRANT SELECT ON TABLES TO analyst_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA internal        GRANT SELECT ON TABLES TO analyst_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA client_mtn      GRANT SELECT ON TABLES TO metabase_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA client_unilever GRANT SELECT ON TABLES TO metabase_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA internal        GRANT SELECT ON TABLES TO metabase_app;

-- Medallion schemas (read-only for analyst/metabase — dbt writes as platform_admin)
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO analyst_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO analyst_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT SELECT ON TABLES TO analyst_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO metabase_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO metabase_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT SELECT ON TABLES TO metabase_app;

-- Lock down public schema
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- ══════════════════════════════════════════
-- SYSTEM & METADATA TABLES (qc_system)
-- ══════════════════════════════════════════

-- Incremental sync state — cursor only advances on confirmed success
CREATE TABLE IF NOT EXISTS qc_system.sync_state (
    pipeline_name        TEXT PRIMARY KEY,
    last_successful_sync TIMESTAMPTZ,
    last_submission_key  TEXT,
    last_run_status      TEXT DEFAULT 'never_run',
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Schema evolution version registry
CREATE TABLE IF NOT EXISTS qc_system.form_versions (
    form_id         TEXT NOT NULL,
    version_hash    TEXT NOT NULL,
    column_manifest JSONB NOT NULL,
    detected_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (form_id, version_hash)
);

-- Centralised QC flags (all automated checks write here)
CREATE TABLE IF NOT EXISTS qc_system.qc_flags (
    id              BIGSERIAL PRIMARY KEY,
    submission_uuid TEXT NOT NULL,
    client_schema   TEXT NOT NULL,
    form_id         TEXT NOT NULL,
    flag_type       TEXT NOT NULL,
    severity        TEXT NOT NULL CHECK (severity IN ('critical', 'high', 'medium', 'low')),
    detail          JSONB NOT NULL,
    flagged_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qc_flags_uuid     ON qc_system.qc_flags (submission_uuid);
CREATE INDEX IF NOT EXISTS idx_qc_flags_severity ON qc_system.qc_flags (severity);
CREATE INDEX IF NOT EXISTS idx_qc_flags_form     ON qc_system.qc_flags (form_id, client_schema);
CREATE INDEX IF NOT EXISTS idx_qc_flags_date     ON qc_system.qc_flags (flagged_at);

-- NDPR compliance — access and system event audit log
CREATE TABLE IF NOT EXISTS qc_system.audit_log (
    id           BIGSERIAL PRIMARY KEY,
    action       TEXT NOT NULL,
    schema_name  TEXT,
    performed_by TEXT DEFAULT CURRENT_USER,
    detail       JSONB,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Dead-letter queue — failed webhook/API payloads preserved for replay
CREATE TABLE IF NOT EXISTS qc_system.failed_payloads (
    id             BIGSERIAL PRIMARY KEY,
    form_id        TEXT NOT NULL,
    client_schema  TEXT NOT NULL,
    raw_payload    JSONB NOT NULL,
    error_message  TEXT,
    retry_count    INTEGER DEFAULT 0,
    status         TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'replayed', 'abandoned')),
    received_at    TIMESTAMPTZ DEFAULT NOW(),
    last_attempted TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_failed_payloads_status ON qc_system.failed_payloads (status);

-- Pipeline SLA tracker — expected cadence and max-lag per pipeline
CREATE TABLE IF NOT EXISTS qc_system.pipeline_sla (
    pipeline_name             TEXT PRIMARY KEY,
    expected_interval_minutes INTEGER NOT NULL,
    max_lag_minutes           INTEGER NOT NULL,
    owner                     TEXT,
    created_at                TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO qc_system.pipeline_sla (pipeline_name, expected_interval_minutes, max_lag_minutes, owner)
VALUES
    ('surveycto_nightly', 1440, 1800, 'data_team'),
    ('nightly_qc',        1440, 1800, 'data_team'),
    ('nightly_dbt',       1440, 1800, 'data_team'),
    ('nightly_backup',    1440, 1800, 'data_team')
ON CONFLICT (pipeline_name) DO NOTHING;

-- Alert suppression — prevents same alert firing repeatedly during an outage
CREATE TABLE IF NOT EXISTS qc_system.alert_suppression (
    alert_key  TEXT PRIMARY KEY,
    last_sent  TIMESTAMPTZ NOT NULL,
    sent_count INTEGER DEFAULT 1
);

-- GPS regional boundaries — loaded by qc_engine.check_gps (not hardcoded)
CREATE TABLE IF NOT EXISTS qc_system.gps_boundaries (
    region   TEXT PRIMARY KEY,
    min_lat  NUMERIC(9,6) NOT NULL,
    max_lat  NUMERIC(9,6) NOT NULL,
    min_lon  NUMERIC(9,6) NOT NULL,
    max_lon  NUMERIC(9,6) NOT NULL
);

INSERT INTO qc_system.gps_boundaries (region, min_lat, max_lat, min_lon, max_lon) VALUES
    ('Lagos',   6.300000,  6.700000,  3.100000,  3.600000),
    ('Abuja',   8.900000,  9.200000,  7.100000,  7.600000),
    ('Kano',   11.800000, 12.200000,  8.300000,  8.800000),
    ('Rivers',  4.700000,  5.000000,  6.900000,  7.400000),
    ('Oyo',     7.200000,  7.700000,  3.800000,  4.300000),
    ('Kaduna',  9.900000, 10.600000,  7.200000,  7.800000)
ON CONFLICT (region) DO NOTHING;

-- Enumerator quality scorecard — populated by qc_engine.compute_scores
CREATE TABLE IF NOT EXISTS qc_system.enumerator_scores (
    enumerator_id     TEXT         NOT NULL,
    form_id           TEXT         NOT NULL,
    client_schema     TEXT         NOT NULL,
    total_submissions INTEGER      DEFAULT 0,
    total_flags       INTEGER      DEFAULT 0,
    critical_flags    INTEGER      DEFAULT 0,
    high_flags        INTEGER      DEFAULT 0,
    medium_flags      INTEGER      DEFAULT 0,
    quality_score     NUMERIC(5,2),
    computed_at       TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (enumerator_id, form_id)
);

CREATE INDEX IF NOT EXISTS idx_enum_scores_form ON qc_system.enumerator_scores (form_id, client_schema);
