# ══════════════════════════════════════════
# Research Data Platform — Python Quality Control Engine
# ══════════════════════════════════════════

import os
import sys                                                          # FIX Bug 1+6: must be at top level
from dotenv import load_dotenv

# ── Path setup (must come before any project-local imports) ──────────────────
# When this script runs as __main__ (e.g. via systemd or direct invocation),
# Python inserts the script's own directory (etl/qc/) onto sys.path[0],
# NOT the project root. That makes `import etl.utils` fail with ModuleNotFoundError.
# We explicitly add the project root so the etl package is always importable.
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))  # FIX Bug 1
if PROJECT_ROOT not in sys.path:                                       # FIX Bug 1
    sys.path.insert(0, PROJECT_ROOT)                                   # FIX Bug 1

ENV_PATH = os.path.join(SCRIPT_DIR, "..", "..", "secrets", ".env")
load_dotenv(ENV_PATH)

import json
from prefect import flow, task, serve
from prefect.logging import get_run_logger
from sqlalchemy import create_engine, text

# FIX: import shared safe_id from utils — removes the duplicate definition that
# previously lived both here and inside sync_surveycto.py's upsert_submissions.
from etl.utils import safe_id as _safe_id, build_db_url

# FIX: was building DB_URL inline without URL-encoding the password.
# Now delegates to etl.utils.build_db_url() which uses urllib.parse.quote_plus.
DB_URL = build_db_url()


@task(name="qc-duplicate-detection")
def check_duplicates(form_id: str, client_schema: str, engine):
    """Flags duplicate phone submissions received on the same day."""
    logger     = get_run_logger()
    safe_schema = _safe_id(client_schema)
    safe_form   = _safe_id(form_id)
    table_name  = f"{safe_schema}.{safe_form}"

    with engine.connect() as conn:
        cols = [r[0] for r in conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"),
            {"s": safe_schema, "t": safe_form}
        ).fetchall()]

        if 'respondent_phone' not in cols or 'submission_date' not in cols:
            logger.info("Duplicates check skipped: fields 'respondent_phone' or 'submission_date' not found in table.")
            return

        logger.info(f"Scanning '{table_name}' for duplicate phone numbers submitted on the same day.")

        sql = f"""
            INSERT INTO qc_system.qc_flags (submission_uuid, client_schema, form_id, flag_type, severity, detail)
            SELECT a.submission_uuid, :client_schema, :form_id, 'duplicate_phone', 'critical',
                jsonb_build_object('matched_uuid', b.submission_uuid, 'phone', a.respondent_phone, 'submission_date', a.submission_date)
            FROM {table_name} a
            JOIN {table_name} b
              ON a.respondent_phone = b.respondent_phone
             AND a.submission_uuid <> b.submission_uuid
             AND a.submission_date::date = b.submission_date::date
            WHERE NOT EXISTS (
                SELECT 1 FROM qc_system.qc_flags f
                WHERE f.submission_uuid = a.submission_uuid AND f.flag_type = 'duplicate_phone'
            )
        """
        conn.execute(text(sql), {"client_schema": client_schema, "form_id": form_id})
        conn.commit()
        logger.info("Duplicate scan completed.")


@task(name="qc-speed-violations")
def check_speed(form_id: str, client_schema: str, engine):
    """Flags submissions that fall below the 10th percentile for interview duration."""
    logger      = get_run_logger()
    safe_schema = _safe_id(client_schema)
    safe_form   = _safe_id(form_id)
    table_name  = f"{safe_schema}.{safe_form}"

    with engine.connect() as conn:
        cols = [r[0] for r in conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"),
            {"s": safe_schema, "t": safe_form}
        ).fetchall()]

        if 'duration_seconds' not in cols:
            logger.info("Speed check skipped: field 'duration_seconds' not found.")
            return

        logger.info(f"Calculating 10th percentile duration for speed flagging on '{table_name}'.")

        sql = f"""
            WITH pct AS (
                SELECT PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY duration_seconds) AS p10
                FROM {table_name}
            )
            INSERT INTO qc_system.qc_flags (submission_uuid, client_schema, form_id, flag_type, severity, detail)
            SELECT f.submission_uuid, :client_schema, :form_id, 'speed_violation', 'high',
                jsonb_build_object('duration_seconds', f.duration_seconds, 'p10_threshold', p.p10)
            FROM {table_name} f, pct p
            WHERE f.duration_seconds < p.p10
              AND NOT EXISTS (
                  SELECT 1 FROM qc_system.qc_flags q
                  WHERE q.submission_uuid = f.submission_uuid AND q.flag_type = 'speed_violation'
              )
        """
        conn.execute(text(sql), {"client_schema": client_schema, "form_id": form_id})
        conn.commit()
        logger.info("Speed violation scan completed.")


@task(name="qc-gps-boundaries")
def check_gps(form_id: str, client_schema: str, engine):
    """Flags GPS coordinates that lie outside regional boundary constraints.

    FIX: boundaries are now loaded from qc_system.gps_boundaries (populated by
    init_db.sql) instead of a hardcoded dict that only covered Lagos/Abuja/Kano.
    Adding a new region is now a one-row INSERT — no code change required.
    """
    logger      = get_run_logger()
    safe_schema = _safe_id(client_schema)
    safe_form   = _safe_id(form_id)
    table_name  = f"{safe_schema}.{safe_form}"

    with engine.connect() as conn:
        cols = [r[0] for r in conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"),
            {"s": safe_schema, "t": safe_form}
        ).fetchall()]

        if not all(col in cols for col in ['latitude', 'longitude', 'region']):
            logger.info("GPS boundaries check skipped: 'latitude', 'longitude', or 'region' columns are absent.")
            return

        # Load boundaries from the config table rather than hardcoded values
        boundary_rows = conn.execute(
            text("SELECT region, min_lat, max_lat, min_lon, max_lon FROM qc_system.gps_boundaries")
        ).fetchall()

        if not boundary_rows:
            logger.warning("GPS check skipped: qc_system.gps_boundaries table is empty.")
            return

        boundaries = {
            row[0]: (float(row[1]), float(row[2]), float(row[3]), float(row[4]))
            for row in boundary_rows
        }

        logger.info(f"Starting GPS boundary scanner for '{table_name}' — {len(boundaries)} regions loaded from DB.")

        records = conn.execute(
            text(f"SELECT submission_uuid, latitude, longitude, region FROM {table_name}")
        ).fetchall()

        flags_inserted = 0
        for rec_uuid, lat, lon, reg in records:
            if not lat or not lon or not reg:
                continue

            box = boundaries.get(reg)
            if not box:
                # Region not in config — skip silently (rather than silently passing as before)
                logger.debug(f"Region '{reg}' has no boundary config — submission {rec_uuid} skipped.")
                continue

            min_lat, max_lat, min_lon, max_lon = box

            if not (min_lat <= float(lat) <= max_lat) or not (min_lon <= float(lon) <= max_lon):
                flag_sql = """
                    INSERT INTO qc_system.qc_flags (submission_uuid, client_schema, form_id, flag_type, severity, detail)
                    SELECT :uuid, :schema, :fid, 'gps_out_of_region', 'high', :detail::jsonb
                    WHERE NOT EXISTS (
                        SELECT 1 FROM qc_system.qc_flags
                        WHERE submission_uuid = :uuid AND flag_type = 'gps_out_of_region'
                    )
                """
                conn.execute(text(flag_sql), {
                    "uuid":   rec_uuid,
                    "schema": client_schema,
                    "fid":    form_id,
                    "detail": json.dumps({
                        "region":    reg,
                        "latitude":  lat,
                        "longitude": lon,
                        "bounds":    {"lat": [min_lat, max_lat], "lon": [min_lon, max_lon]}
                    })
                })
                flags_inserted += 1

        if flags_inserted > 0:
            conn.commit()

        logger.info(f"GPS boundaries scan completed. Raised {flags_inserted} flags.")


@task(name="qc-statistical-outliers")
def check_outliers(form_id: str, client_schema: str, engine):
    """Flags values exceeding a standard deviation Z-score threshold of 3."""
    logger      = get_run_logger()
    safe_schema = _safe_id(client_schema)
    safe_form   = _safe_id(form_id)
    table_name  = f"{safe_schema}.{safe_form}"

    with engine.connect() as conn:
        cols = [r[0] for r in conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"),
            {"s": safe_schema, "t": safe_form}
        ).fetchall()]

        numeric_candidates = ['income', 'amount_spent', 'household_size', 'age']
        numeric_cols       = [c for c in numeric_candidates if c in cols]

        if not numeric_cols:
            logger.info("Statistical outliers check skipped: no target numeric columns found.")
            return

        logger.info(f"Analyzing outlier metrics (Z-score > 3) for variables: {numeric_cols}")

        for column in numeric_cols:
            sql = f"""
                WITH scored AS (
                    SELECT submission_uuid,
                        "{column}"::numeric AS value,
                        ( "{column}"::numeric - AVG("{column}"::numeric) OVER() ) /
                        NULLIF(STDDEV("{column}"::numeric) OVER(), 0) AS z
                    FROM {table_name}
                    WHERE "{column}" IS NOT NULL
                )
                INSERT INTO qc_system.qc_flags (submission_uuid, client_schema, form_id, flag_type, severity, detail)
                SELECT submission_uuid, :client_schema, :form_id, 'statistical_outlier', 'medium',
                    jsonb_build_object('field', :col, 'value', value, 'z_score', ROUND(z::numeric, 2))
                FROM scored
                WHERE ABS(z) > 3
                  AND NOT EXISTS (
                      SELECT 1 FROM qc_system.qc_flags q
                      WHERE q.submission_uuid = scored.submission_uuid
                        AND q.flag_type = 'statistical_outlier'
                        AND q.detail->>'field' = :col
                  )
            """
            conn.execute(text(sql), {"client_schema": client_schema, "form_id": form_id, "col": column})
            conn.commit()

        logger.info("Statistical outlier scan completed.")


@task(name="qc-import-native-rejections")
def import_rejections(form_id: str, client_schema: str, engine):
    """Pulls native SurveyCTO 'rejected' records and catalogs them in qc_flags."""
    logger      = get_run_logger()
    safe_schema = _safe_id(client_schema)
    safe_form   = _safe_id(form_id)
    table_name  = f"{safe_schema}.{safe_form}"

    with engine.connect() as conn:
        cols = [r[0] for r in conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"),
            {"s": safe_schema, "t": safe_form}
        ).fetchall()]

        if 'review_status' not in cols:
            return

        logger.info(f"Syncing manual rejections from review console in '{table_name}' to qc_flags.")

        sql = f"""
            INSERT INTO qc_system.qc_flags (submission_uuid, client_schema, form_id, flag_type, severity, detail)
            SELECT t.submission_uuid, :client_schema, :form_id, 'surveycto_rejected', 'high',
                jsonb_build_object('review_status', t.review_status, 'source', 'SurveyCTO Review Console')
            FROM {table_name} t
            WHERE t.review_status = 'rejected'
              AND NOT EXISTS (
                  SELECT 1 FROM qc_system.qc_flags q
                  WHERE q.submission_uuid = t.submission_uuid
                    AND q.flag_type = 'surveycto_rejected'
              )
        """
        conn.execute(text(sql), {"client_schema": client_schema, "form_id": form_id})
        conn.commit()
        logger.info("Manual console rejections sync completed.")


@task(name="qc-update-leaderboards")
def compute_scores(form_id: str, client_schema: str, engine):
    """Recalculates quality scorecard tallies for enumerators.

    FIX Bug 2: flag_counts CTE previously used f.detail->>'enumerator_id' which
    is never populated by any check_* function — every flag returned NULL, giving
    every enumerator 0 flags regardless of reality.

    Fix: JOIN qc_flags back to the form table on submission_uuid to read the
    enumerator identifier column directly from the survey row. The column name is
    determined at Python time via the enumerator_col probe below, so the f-string
    interpolation is safe (the value has already been sanitised by _safe_id).

    The enumerator identifier column is probed in priority order:
    enumerator_id → enumeratorid → deviceid → username
    """
    logger      = get_run_logger()
    safe_schema = _safe_id(client_schema)
    safe_form   = _safe_id(form_id)
    table_name  = f"{safe_schema}.{safe_form}"

    with engine.connect() as conn:
        cols = [r[0] for r in conn.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"),
            {"s": safe_schema, "t": safe_form}
        ).fetchall()]

        # Probe common enumerator ID column names
        enumerator_col = next(
            (c for c in ['enumerator_id', 'enumeratorid', 'deviceid', 'username'] if c in cols),
            None
        )

        if enumerator_col is None:
            logger.info(f"compute_scores skipped for {form_id}: no enumerator ID column found.")
            return

        logger.info(f"Computing enumerator scorecards for '{table_name}' using column '{enumerator_col}'.")

        # FIX Bug 2: flag_counts now JOINs qc_flags to the form table so the
        # enumerator_id comes from the actual survey row, not from detail JSONB
        # (which never contained this key).
        sql = f"""
            WITH submission_counts AS (
                SELECT "{enumerator_col}" AS enumerator_id,
                       COUNT(*) AS total_submissions
                FROM {table_name}
                WHERE "{enumerator_col}" IS NOT NULL
                GROUP BY "{enumerator_col}"
            ),
            flag_counts AS (
                SELECT
                    t."{enumerator_col}"                                         AS enumerator_id,
                    COUNT(*)                                                      AS total_flags,
                    COUNT(*) FILTER (WHERE f.severity = 'critical')              AS critical_flags,
                    COUNT(*) FILTER (WHERE f.severity = 'high')                  AS high_flags,
                    COUNT(*) FILTER (WHERE f.severity = 'medium')                AS medium_flags
                FROM qc_system.qc_flags f
                JOIN {table_name} t ON t.submission_uuid = f.submission_uuid
                WHERE f.form_id       = :form_id
                  AND f.client_schema  = :client_schema
                  AND t."{enumerator_col}" IS NOT NULL
                GROUP BY t."{enumerator_col}"
            ),
            scored AS (
                SELECT
                    s.enumerator_id,
                    s.total_submissions,
                    COALESCE(fl.total_flags,    0) AS total_flags,
                    COALESCE(fl.critical_flags, 0) AS critical_flags,
                    COALESCE(fl.high_flags,     0) AS high_flags,
                    COALESCE(fl.medium_flags,   0) AS medium_flags,
                    -- Quality score: 100 minus weighted flag penalty, floored at 0
                    GREATEST(0, ROUND(
                        100.0
                        - (COALESCE(fl.critical_flags, 0) * 10.0)
                        - (COALESCE(fl.high_flags,     0) * 5.0)
                        - (COALESCE(fl.medium_flags,   0) * 2.0)
                    , 2)) AS quality_score
                FROM submission_counts s
                LEFT JOIN flag_counts fl USING (enumerator_id)
            )
            INSERT INTO qc_system.enumerator_scores
                (enumerator_id, form_id, client_schema,
                 total_submissions, total_flags, critical_flags, high_flags, medium_flags,
                 quality_score, computed_at)
            SELECT
                enumerator_id, :form_id, :client_schema,
                total_submissions, total_flags, critical_flags, high_flags, medium_flags,
                quality_score, NOW()
            FROM scored
            ON CONFLICT (enumerator_id, form_id) DO UPDATE SET
                total_submissions = EXCLUDED.total_submissions,
                total_flags       = EXCLUDED.total_flags,
                critical_flags    = EXCLUDED.critical_flags,
                high_flags        = EXCLUDED.high_flags,
                medium_flags      = EXCLUDED.medium_flags,
                quality_score     = EXCLUDED.quality_score,
                computed_at       = NOW()
        """
        result = conn.execute(text(sql), {"form_id": form_id, "client_schema": client_schema})
        conn.commit()
        logger.info(f"Enumerator scorecards updated ({result.rowcount} enumerators).")


@flow(name="quality-control-engine", log_prints=True)
def run_qc(form_id: str, client_schema: str):
    """Primary entry point for automated survey data quality assessments."""
    logger = get_run_logger()
    logger.info(f"🔍 Starting Quality Control assessment flow for {client_schema}.{str(form_id)}")

    engine = create_engine(DB_URL)

    check_duplicates(form_id, client_schema, engine)
    check_speed(form_id, client_schema, engine)
    check_gps(form_id, client_schema, engine)
    check_outliers(form_id, client_schema, engine)
    import_rejections(form_id, client_schema, engine)
    compute_scores(form_id, client_schema, engine)
    logger.info("✨ Quality checking assessment process completed for " + str(form_id) + ".")


if __name__ == "__main__":
    # FIX Bug 6: `import sys` was here before; sys is now imported at module level above.
    if len(sys.argv) > 2:
        # Direct invocation: python qc_engine.py <form_id> <schema>
        run_qc(sys.argv[1], sys.argv[2])
    else:
        # FIX: serve ALL three forms on the nightly schedule.
        # Previously only project_appraise / client_mtn was registered.
        dep_mtn = run_qc.to_deployment(
            name="qc-engine-nightly-mtn",
            cron="30 1 * * *",
            parameters={"form_id": "project_appraise", "client_schema": "client_mtn"}
        )
        dep_unilever = run_qc.to_deployment(
            name="qc-engine-nightly-unilever",
            cron="30 1 * * *",
            parameters={"form_id": "unilever-retail", "client_schema": "client_unilever"}
        )
        dep_internal = run_qc.to_deployment(
            name="qc-engine-nightly-internal",
            cron="30 1 * * *",
            parameters={"form_id": "internal-census", "client_schema": "internal"}
        )
        serve(dep_mtn, dep_unilever, dep_internal)
