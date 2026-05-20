-- ══════════════════════════════════════════
-- Research Data Platform — Initial Database Setup
-- ══════════════════════════════════════════

-- Create Client Schemas (isolation layers)
CREATE SCHEMA IF NOT EXISTS client_mtn;
CREATE SCHEMA IF NOT EXISTS client_unilever;
CREATE SCHEMA IF NOT EXISTS internal;
CREATE SCHEMA IF NOT EXISTS qc_system;

-- Create Roles
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_writer') THEN
        CREATE ROLE etl_writer;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_reader') THEN
        CREATE ROLE analyst_reader;
    END IF;
END
$$;

-- Grant Schema Permissions
GRANT USAGE ON SCHEMA client_mtn, client_unilever, internal, qc_system TO etl_writer;
GRANT USAGE ON SCHEMA client_mtn, client_unilever, internal TO analyst_reader;

-- Service User Accounts
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_svc') THEN
        CREATE USER etl_svc WITH PASSWORD 'EtlService!Str0ng2026';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst') THEN
        CREATE USER analyst WITH PASSWORD 'Analyst!Str0ngReader2026';
    END IF;
END
$$;

GRANT etl_writer TO etl_svc;
GRANT analyst_reader TO analyst;

-- Configure default permissions for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA client_mtn GRANT INSERT, UPDATE, SELECT, DELETE ON TABLES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA client_unilever GRANT INSERT, UPDATE, SELECT, DELETE ON TABLES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA internal GRANT INSERT, UPDATE, SELECT, DELETE ON TABLES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA qc_system GRANT INSERT, UPDATE, SELECT, DELETE ON TABLES TO etl_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA client_mtn GRANT SELECT ON TABLES TO analyst_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA client_unilever GRANT SELECT ON TABLES TO analyst_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA internal GRANT SELECT ON TABLES TO analyst_reader;

-- ══════════════════════════════════════════
-- SYSTEM & METADATA TABLES (qc_system)
-- ══════════════════════════════════════════

-- Incremental Sync State Tracker
CREATE TABLE IF NOT EXISTS qc_system.sync_state (
    pipeline_name        TEXT PRIMARY KEY,
    last_successful_sync TIMESTAMPTZ,
    last_submission_key  TEXT,
    last_run_status      TEXT DEFAULT 'never_run',
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Schema Evolution Version Registry
CREATE TABLE IF NOT EXISTS qc_system.form_versions (
    form_id         TEXT NOT NULL,
    version_hash    TEXT NOT NULL,
    column_manifest JSONB NOT NULL,
    detected_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (form_id, version_hash)
);

-- Centralized Quality Control Flags Table
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

CREATE INDEX IF NOT EXISTS idx_qc_flags_uuid ON qc_system.qc_flags (submission_uuid);
CREATE INDEX IF NOT EXISTS idx_qc_flags_severity ON qc_system.qc_flags (severity);

-- NDPR Compliance Access & System Event Audit Log
CREATE TABLE IF NOT EXISTS qc_system.audit_log (
    id           BIGSERIAL PRIMARY KEY,
    action       TEXT NOT NULL,
    schema_name  TEXT,
    performed_by TEXT DEFAULT CURRENT_USER,
    detail       JSONB,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Dead Letter Queue (DLQ) for failed webhook/API loads
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

-- Ingestion/Pipeline SLA Tracker
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

-- Quality Alert Suppression Table
CREATE TABLE IF NOT EXISTS qc_system.alert_suppression (
    alert_key  TEXT PRIMARY KEY,
    last_sent  TIMESTAMPTZ NOT NULL,
    sent_count INTEGER DEFAULT 1
);
