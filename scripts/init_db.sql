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

CREATE TABLE IF NOT EXISTS qc_system.registered_forms (
    form_id        TEXT PRIMARY KEY,
    client         TEXT NOT NULL,
    client_schema  TEXT NOT NULL,
    table_name     TEXT NOT NULL,
    active         BOOLEAN NOT NULL DEFAULT TRUE,
    etl_deployment TEXT,
    qc_deployment  TEXT,
    registered_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS qc_system.project_state_quotas (
    form_id       TEXT NOT NULL,
    client_schema TEXT NOT NULL,
    state_name    TEXT NOT NULL,
    quota_target  INTEGER NOT NULL CHECK (quota_target >= 0),
    wave_name     TEXT NOT NULL DEFAULT 'default',
    active        BOOLEAN NOT NULL DEFAULT TRUE,
    notes         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (form_id, client_schema, state_name, wave_name)
);

CREATE INDEX IF NOT EXISTS idx_project_state_quotas_active
    ON qc_system.project_state_quotas (active, form_id, state_name);

CREATE OR REPLACE FUNCTION qc_system.table_row_count(
    schema_name TEXT,
    table_name TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    result BIGINT;
BEGIN
    IF to_regclass(format('%I.%I', schema_name, table_name)) IS NULL THEN
        RETURN 0;
    END IF;

    EXECUTE format('SELECT count(*) FROM %I.%I', schema_name, table_name)
    INTO result;

    RETURN COALESCE(result, 0);
END
$$;

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

-- ── Haversine distance helper ─────────────────────────────────────────────
-- Returns the great-circle distance in metres between two WGS-84 coordinates.
-- Used by qc_engine GPS Phase 2 (respondent proximity) entirely inside the DB
-- so no floating-point round-tripping through Python is needed.
-- IMMUTABLE + STRICT: PostgreSQL can cache results and short-circuit on NULLs.
CREATE OR REPLACE FUNCTION qc_system.haversine_metres(
    lat1 NUMERIC, lon1 NUMERIC,
    lat2 NUMERIC, lon2 NUMERIC
) RETURNS NUMERIC AS $$
    SELECT ROUND(
        6371000.0 * 2.0 * ASIN(SQRT(
            POWER(SIN(RADIANS((lat2 - lat1) / 2.0)), 2)
            + COS(RADIANS(lat1))
            * COS(RADIANS(lat2))
            * POWER(SIN(RADIANS((lon2 - lon1) / 2.0)), 2)
        ))
    ::NUMERIC, 1)
$$ LANGUAGE SQL IMMUTABLE STRICT;

-- ── Respondent field locations (sample frame) ─────────────────────────────
-- Populated BEFORE each survey wave by loading the approved sample frame.
-- Each row registers where a specific respondent is physically expected to be
-- found during fieldwork. The GPS Phase 2 check measures the distance between
-- the coordinates recorded on the device and this registered location; anything
-- beyond tolerance_metres is flagged as gps_wrong_field_location (critical).
--
-- How to load for a new wave:
--   COPY qc_system.respondent_locations
--     (respondent_id, form_id, client_schema, expected_lat, expected_lon,
--      location_name, tolerance_metres)
--   FROM '/path/to/sample_frame.csv' CSV HEADER;
--
-- tolerance_metres guidance:
--   200 m  — urban areas with dense housing (Lagos Island, Kano Municipal)
--   500 m  — peri-urban or areas with imprecise address data
--   1000 m — rural areas where household GPS was collected by hand last season
CREATE TABLE IF NOT EXISTS qc_system.respondent_locations (
    respondent_id     TEXT         NOT NULL,
    form_id           TEXT         NOT NULL,
    client_schema     TEXT         NOT NULL,
    expected_lat      NUMERIC(9,6) NOT NULL,
    expected_lon      NUMERIC(9,6) NOT NULL,
    location_name     TEXT,                          -- human-readable label (LGA, ward, address)
    tolerance_metres  INTEGER      NOT NULL DEFAULT 200,
    created_at        TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (respondent_id, form_id, client_schema)
);

CREATE INDEX IF NOT EXISTS idx_respondent_locations_form
    ON qc_system.respondent_locations (form_id, client_schema);

GRANT INSERT, UPDATE, SELECT, DELETE
    ON qc_system.respondent_locations TO etl_writer;

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
