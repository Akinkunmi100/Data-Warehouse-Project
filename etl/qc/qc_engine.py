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
from prefect.cache_policies import NO_CACHE
from prefect.logging import get_run_logger
from sqlalchemy import create_engine, text

# FIX: import shared safe_id from utils — removes the duplicate definition that
# previously lived both here and inside sync_surveycto.py's upsert_submissions.
from etl.surveycto_registry import get_form_config, load_form_registry
from etl.utils import safe_id as _safe_id, build_db_url

# FIX: was building DB_URL inline without URL-encoding the password.
# Now delegates to etl.utils.build_db_url() which uses urllib.parse.quote_plus.
DB_URL = build_db_url()


@task(name="qc-duplicate-detection", cache_policy=NO_CACHE)
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


@task(name="qc-speed-violations", cache_policy=NO_CACHE)
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


@task(name="qc-gps-boundaries", cache_policy=NO_CACHE)
def check_gps(form_id: str, client_schema: str, engine):
    """
    Three-phase GPS integrity check.

    Phase 1 — Regional boundary
      The original coarse check. Verifies that the coordinates recorded on the
      device fall within the bounding box of the region the enumerator declared.
      Catches gross mismatches (e.g. device GPS says Kano but the enumerator
      is assigned to Lagos). Fast, pure-SQL, runs even with no sample frame.
      Flag type : gps_out_of_region
      Severity  : high

    Phase 2 — Respondent field proximity
      The core enhancement. Compares the recorded GPS against the pre-registered
      field location of the specific respondent in qc_system.respondent_locations
      (loaded from the approved sample frame before each survey wave).
      Even an enumerator who is correctly in Lagos will be flagged if their
      device GPS is 600 m from the respondent's household — the most common
      signature of home-filling within the correct region.
      Distance is computed with qc_system.haversine_metres(), a PostgreSQL
      IMMUTABLE SQL function that avoids Python float round-tripping.
      Each respondent row carries its own tolerance_metres so that urban
      (200 m), peri-urban (500 m), and rural (1 000 m) areas are handled
      without a single global cutoff.
      Flag type : gps_wrong_field_location
      Severity  : critical
      Skipped gracefully when qc_system.respondent_locations is empty (i.e.
      before the sample frame has been loaded for a new wave).

    Phase 3 — Enumerator GPS clustering (home-filling detection)
      A behavioural pattern check that operates at the enumerator level.
      If an enumerator's submissions are all tightly clustered in one spot —
      every interview GPS within 50 m of every other — they are almost
      certainly sitting in one fixed location (their home, a tea shop, a
      field office) and fabricating respondent details rather than visiting
      households.

      Method: compute the standard deviation of latitude and longitude across
      all submissions per enumerator, convert both to metres using:
          lat_stddev_m = stddev_lat × 111 320
          lon_stddev_m = stddev_lon × 111 320 × cos(mean_lat)
      Flag any enumerator where BOTH are below 50 m and they have at least
      MIN_CLUSTER_SUBMISSIONS submissions (default 5). Fewer than 5 submissions
      is insufficient evidence — a new enumerator on day one may legitimately
      have only visited one compound.

      All submissions from that enumerator receive the flag so analysts can
      see the full clustered set in one filter. The detail JSONB includes the
      cluster centroid, both stddev values in metres, and the submission count
      so the severity of the pattern is immediately visible.
      Flag type : gps_enumerator_clustering
      Severity  : critical
    """
    logger      = get_run_logger()
    safe_schema = _safe_id(client_schema)
    safe_form   = _safe_id(form_id)
    table_name  = f"{safe_schema}.{safe_form}"

    # Minimum number of submissions before the clustering check fires.
    # Below this, a tight cluster could just be a new enumerator starting work.
    MIN_CLUSTER_SUBMISSIONS = 5
    # Clustering threshold in metres. If BOTH lat and lon stddev are below this
    # the enumerator is considered to be submitting from a single fixed point.
    CLUSTER_RADIUS_M = 50.0

    with engine.connect() as conn:

        # ── Column inventory (one round-trip, shared by all three phases) ────────
        cols = [r[0] for r in conn.execute(
            text("SELECT column_name FROM information_schema.columns "
                 "WHERE table_schema=:s AND table_name=:t"),
            {"s": safe_schema, "t": safe_form}
        ).fetchall()]

        has_gps = all(c in cols for c in ['latitude', 'longitude'])
        if not has_gps:
            logger.info("GPS check skipped: 'latitude' or 'longitude' columns are absent.")
            return

        has_region       = 'region' in cols
        respondent_col   = next(
            (c for c in ['respondent_id', 'respondent_uuid', 'household_id', 'hh_id', 'case_id'] if c in cols),
            None
        )
        enumerator_col   = next(
            (c for c in ['enumerator_id', 'enumeratorid', 'deviceid', 'username'] if c in cols),
            None
        )

        # ───────────────────────────────────────────────────────────────────
        # PHASE 1 — REGIONAL BOUNDARY
        # ───────────────────────────────────────────────────────────────────
        if not has_region:
            logger.info("GPS Phase 1 skipped: 'region' column absent.")
        else:
            boundary_rows = conn.execute(
                text("SELECT region, min_lat, max_lat, min_lon, max_lon FROM qc_system.gps_boundaries")
            ).fetchall()

            if not boundary_rows:
                logger.warning("GPS Phase 1 skipped: qc_system.gps_boundaries is empty.")
            else:
                boundaries = {
                    row[0]: (float(row[1]), float(row[2]), float(row[3]), float(row[4]))
                    for row in boundary_rows
                }
                logger.info(
                    f"GPS Phase 1 — regional boundary scan on '{table_name}' "
                    f"({len(boundaries)} regions loaded)."
                )

                records = conn.execute(
                    text(f"SELECT submission_uuid, latitude, longitude, region FROM {table_name}")
                ).fetchall()

                p1_flags = 0
                for rec_uuid, lat, lon, reg in records:
                    if not lat or not lon or not reg:
                        continue
                    box = boundaries.get(reg)
                    if not box:
                        logger.debug(f"Phase 1: region '{reg}' has no boundary config — {rec_uuid} skipped.")
                        continue
                    min_lat, max_lat, min_lon, max_lon = box
                    if not (min_lat <= float(lat) <= max_lat) or not (min_lon <= float(lon) <= max_lon):
                        conn.execute(text("""
                            INSERT INTO qc_system.qc_flags
                                (submission_uuid, client_schema, form_id, flag_type, severity, detail)
                            SELECT :uuid, :schema, :fid, 'gps_out_of_region', 'high', CAST(:detail AS jsonb)
                            WHERE NOT EXISTS (
                                SELECT 1 FROM qc_system.qc_flags
                                WHERE submission_uuid = :uuid AND flag_type = 'gps_out_of_region'
                            )
                        """), {
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
                        p1_flags += 1

                if p1_flags:
                    conn.commit()
                logger.info(f"GPS Phase 1 complete — {p1_flags} out-of-region flags raised.")

        # ───────────────────────────────────────────────────────────────────
        # PHASE 2 — RESPONDENT FIELD PROXIMITY
        # Requires: respondent_col present in form table AND
        #           qc_system.respondent_locations populated for this wave.
        # ───────────────────────────────────────────────────────────────────
        if respondent_col is None:
            logger.info(
                "GPS Phase 2 skipped: no respondent identifier column found "
                "(looked for: respondent_id, respondent_uuid, household_id, hh_id, case_id)."
            )
        else:
            frame_count = conn.execute(
                text("SELECT COUNT(*) FROM qc_system.respondent_locations "
                     "WHERE form_id = :fid AND client_schema = :schema"),
                {"fid": form_id, "schema": client_schema}
            ).scalar()

            if not frame_count:
                logger.warning(
                    "GPS Phase 2 skipped: qc_system.respondent_locations has no rows for "
                    f"form_id='{form_id}', client_schema='{client_schema}'. "
                    "Load the sample frame before running QC."
                )
            else:
                logger.info(
                    f"GPS Phase 2 — respondent proximity check on '{table_name}' "
                    f"using {frame_count} registered field locations "
                    f"(respondent column: '{respondent_col}')."
                )

                # haversine_metres() is a PostgreSQL IMMUTABLE SQL function defined in
                # init_db.sql. Running the distance calculation inside the DB avoids
                # fetching all coordinates into Python and eliminates float round-tripping.
                proximity_sql = f"""
                    INSERT INTO qc_system.qc_flags
                        (submission_uuid, client_schema, form_id, flag_type, severity, detail)
                    SELECT
                        f.submission_uuid,
                        :client_schema,
                        :form_id,
                        'gps_wrong_field_location',
                        'critical',
                        jsonb_build_object(
                            'respondent_id',    f."{respondent_col}",
                            'recorded_lat',     f.latitude,
                            'recorded_lon',     f.longitude,
                            'expected_lat',     r.expected_lat,
                            'expected_lon',     r.expected_lon,
                            'location_name',    r.location_name,
                            'distance_metres',  qc_system.haversine_metres(
                                                    f.latitude::numeric,  f.longitude::numeric,
                                                    r.expected_lat,       r.expected_lon
                                                ),
                            'tolerance_metres', r.tolerance_metres
                        )
                    FROM {table_name} f
                    JOIN qc_system.respondent_locations r
                      ON r.respondent_id   = f."{respondent_col}"::text
                     AND r.form_id         = :form_id
                     AND r.client_schema   = :client_schema
                    WHERE f.latitude  IS NOT NULL
                      AND f.longitude IS NOT NULL
                      AND qc_system.haversine_metres(
                              f.latitude::numeric,  f.longitude::numeric,
                              r.expected_lat,       r.expected_lon
                          ) > r.tolerance_metres
                      AND NOT EXISTS (
                          SELECT 1 FROM qc_system.qc_flags q
                          WHERE q.submission_uuid = f.submission_uuid
                            AND q.flag_type = 'gps_wrong_field_location'
                      )
                """
                result = conn.execute(
                    text(proximity_sql),
                    {"client_schema": client_schema, "form_id": form_id}
                )
                conn.commit()
                logger.info(
                    f"GPS Phase 2 complete — {result.rowcount} wrong-field-location flags raised."
                )

        # ───────────────────────────────────────────────────────────────────
        # PHASE 3 — ENUMERATOR GPS CLUSTERING (HOME-FILLING DETECTION)
        # Requires: enumerator_col present in form table.
        # ───────────────────────────────────────────────────────────────────
        if enumerator_col is None:
            logger.info(
                "GPS Phase 3 skipped: no enumerator identifier column found "
                "(looked for: enumerator_id, enumeratorid, deviceid, username)."
            )
        else:
            logger.info(
                f"GPS Phase 3 — enumerator clustering scan on '{table_name}' "
                f"(column: '{enumerator_col}', min submissions: {MIN_CLUSTER_SUBMISSIONS}, "
                f"radius threshold: {CLUSTER_RADIUS_M} m)."
            )

            # Compute per-enumerator spread in metres.
            # stddev_lat × 111 320 gives the north-south spread in metres.
            # stddev_lon × 111 320 × cos(mean_lat) corrects for meridian convergence
            # so the east-west spread is also in metres at the survey latitude.
            # Both must be below CLUSTER_RADIUS_M to be flagged — requiring both
            # axes guards against a legitimate north-south transect (linear route)
            # that has low lon spread but realistic lat spread.
            clustering_sql = f"""
                WITH enumerator_stats AS (
                    SELECT
                        "{enumerator_col}"                              AS enumerator_id,
                        COUNT(*)                                        AS submission_count,
                        AVG(latitude::numeric)                          AS mean_lat,
                        AVG(longitude::numeric)                         AS mean_lon,
                        STDDEV(latitude::numeric)  * 111320.0           AS stddev_lat_m,
                        STDDEV(longitude::numeric) * 111320.0
                            * COS(RADIANS(AVG(latitude::numeric)))      AS stddev_lon_m
                    FROM {table_name}
                    WHERE latitude  IS NOT NULL
                      AND longitude IS NOT NULL
                      AND "{enumerator_col}" IS NOT NULL
                    GROUP BY "{enumerator_col}"
                    HAVING COUNT(*) >= :min_submissions
                ),
                clustered_enumerators AS (
                    SELECT *
                    FROM enumerator_stats
                    WHERE stddev_lat_m < :radius_m
                      AND stddev_lon_m < :radius_m
                )
                INSERT INTO qc_system.qc_flags
                    (submission_uuid, client_schema, form_id, flag_type, severity, detail)
                SELECT
                    t.submission_uuid,
                    :client_schema,
                    :form_id,
                    'gps_enumerator_clustering',
                    'critical',
                    jsonb_build_object(
                        'enumerator_id',     c.enumerator_id,
                        'submission_count',  c.submission_count,
                        'cluster_centre',    jsonb_build_object(
                                                 'latitude',  ROUND(c.mean_lat::numeric, 6),
                                                 'longitude', ROUND(c.mean_lon::numeric, 6)
                                             ),
                        'spread_metres',     jsonb_build_object(
                                                 'latitude_stddev',  ROUND(c.stddev_lat_m::numeric, 1),
                                                 'longitude_stddev', ROUND(c.stddev_lon_m::numeric, 1)
                                             ),
                        'threshold_metres',  :radius_m,
                        'interpretation',    'All submissions originate from a single fixed point. '
                                             'Likely home-filling or data fabrication.'
                    )
                FROM clustered_enumerators c
                JOIN {table_name} t
                  ON t."{enumerator_col}" = c.enumerator_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM qc_system.qc_flags q
                    WHERE q.submission_uuid = t.submission_uuid
                      AND q.flag_type = 'gps_enumerator_clustering'
                )
            """
            result = conn.execute(
                text(clustering_sql),
                {
                    "client_schema":   client_schema,
                    "form_id":         form_id,
                    "min_submissions": MIN_CLUSTER_SUBMISSIONS,
                    "radius_m":        CLUSTER_RADIUS_M,
                }
            )
            conn.commit()
            logger.info(
                f"GPS Phase 3 complete — {result.rowcount} clustering flags raised "
                f"(across all submissions from clustered enumerators)."
            )


@task(name="qc-statistical-outliers", cache_policy=NO_CACHE)
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


@task(name="qc-import-native-rejections", cache_policy=NO_CACHE)
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


@task(name="qc-update-leaderboards", cache_policy=NO_CACHE)
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
    elif len(sys.argv) > 1:
        # Direct invocation: python qc_engine.py <form_id>
        form_id = sys.argv[1]
        config = get_form_config(form_id, active_only=False)
        if not config:
            raise SystemExit(
                f"Form '{form_id}' is not registered. Run: "
                f"python scripts/register_surveycto_form.py {form_id}"
            )
        run_qc(form_id, config["schema"])
    else:
        deployments = []
        for form_id, config in load_form_registry(active_only=True).items():
            deployments.append(
                run_qc.to_deployment(
                    name=config["qc_deployment"],
                    cron=config["qc_cron"],
                    parameters={"form_id": form_id, "client_schema": config["schema"]},
                )
            )
        if not deployments:
            raise SystemExit("No active SurveyCTO forms registered.")
        serve(*deployments)
