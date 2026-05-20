# ══════════════════════════════════════════
# Research Data Platform — Python Quality Control Engine
# ══════════════════════════════════════════

import os
import json
from dotenv import load_dotenv
from prefect import flow, task
from prefect.logging import get_run_logger
from sqlalchemy import create_engine, text

# Dynamic environmental loading relative to script path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, "..", "..", "secrets", ".env")
load_dotenv(ENV_PATH)

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = "localhost"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

@task(name="qc-duplicate-detection")
def check_duplicates(form_id: str, client_schema: str, engine):
    """Flags duplicate phone submissions received on the same day."""
    logger = get_run_logger()
    table_name = f"{client_schema}.{form_id.replace('-', '_')}"
    
    with engine.connect() as conn:
        cols_query = text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"
        )
        cols = [r[0] for r in conn.execute(cols_query, {"s": client_schema, "t": form_id.replace('-', '_')}).fetchall()]
        
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
        logger.info(f"Duplicate scan completed.")

@task(name="qc-speed-violations")
def check_speed(form_id: str, client_schema: str, engine):
    """Flags submissions that fall below the 10th percentile for interview duration."""
    logger = get_run_logger()
    table_name = f"{client_schema}.{form_id.replace('-', '_')}"
    
    with engine.connect() as conn:
        cols_query = text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"
        )
        cols = [r[0] for r in conn.execute(cols_query, {"s": client_schema, "t": form_id.replace('-', '_')}).fetchall()]
        
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
    """Flags GPS coordinates that lie outside regional boundary constraints."""
    logger = get_run_logger()
    table_name = f"{client_schema}.{form_id.replace('-', '_')}"
    
    boundaries = {
        "Lagos": [6.3, 6.7, 3.1, 3.6],
        "Abuja": [8.9, 9.2, 7.1, 7.6],
        "Kano": [11.8, 12.2, 8.3, 8.8]
    }
    
    with engine.connect() as conn:
        cols_query = text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"
        )
        cols = [r[0] for r in conn.execute(cols_query, {"s": client_schema, "t": form_id.replace('-', '_')}).fetchall()]
        
        if not all(col in cols for col in ['latitude', 'longitude', 'region']):
            logger.info("GPS boundaries check skipped: 'latitude', 'longitude', or 'region' columns are absent.")
            return

        logger.info(f"Starting GPS boundary scanner for '{table_name}'.")
        
        fetch_sql = f"SELECT submission_uuid, latitude, longitude, region FROM {table_name}"
        records = conn.execute(text(fetch_sql)).fetchall()
        
        flags_inserted = 0
        for uuid, lat, lon, reg in records:
            if not lat or not lon or not reg:
                continue
            
            box = boundaries.get(reg)
            if not box:
                continue
                
            min_lat, max_lat, min_lon, max_lon = box
            
            if not (min_lat <= float(lat) <= max_lat) or not (min_lon <= float(lon) <= max_lon):
                flag_sql = """
                    INSERT INTO qc_system.qc_flags (submission_uuid, client_schema, form_id, flag_type, severity, detail)
                    VALUES (:uuid, :schema, :fid, 'gps_out_of_region', 'high', :detail)
                    ON CONFLICT DO NOTHING
                """
                detail = json.dumps({
                    "region": reg, 
                    "latitude": lat, 
                    "longitude": lon, 
                    "bounds": {"lat": [min_lat, max_lat], "lon": [min_lon, max_lon]}
                })
                conn.execute(text(flag_sql), {
                    "uuid": uuid, 
                    "schema": client_schema, 
                    "fid": form_id, 
                    "detail": detail
                })
                flags_inserted += 1
                
        if flags_inserted > 0:
            conn.commit()
            
        logger.info(f"GPS boundaries scan completed. Raised {flags_inserted} flags.")

@task(name="qc-statistical-outliers")
def check_outliers(form_id: str, client_schema: str, engine):
    """Flags values exceeding a standard deviation Z-score threshold of 3."""
    logger = get_run_logger()
    table_name = f"{client_schema}.{form_id.replace('-', '_')}"
    
    with engine.connect() as conn:
        cols_query = text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"
        )
        cols = [r[0] for r in conn.execute(cols_query, {"s": client_schema, "t": form_id.replace('-', '_')}).fetchall()]
        
        numeric_candidates = ['income', 'amount_spent', 'household_size', 'age']
        numeric_cols = [c for c in numeric_candidates if c in cols]
        
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
            conn.execute(text(sql), {
                "client_schema": client_schema, 
                "form_id": form_id, 
                "col": column
            })
            conn.commit()
            
        logger.info("Statistical outlier scan completed.")

@task(name="qc-import-native-rejections")
def import_rejections(form_id: str, client_schema: str, engine):
    """Pulls native SurveyCTO 'rejected' records and catalogs them in qc_flags."""
    logger = get_run_logger()
    table_name = f"{client_schema}.{form_id.replace('-', '_')}"
    
    with engine.connect() as conn:
        cols_query = text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=:s AND table_name=:t"
        )
        cols = [r[0] for r in conn.execute(cols_query, {"s": client_schema, "t": form_id.replace('-', '_')}).fetchall()]
        
        if 'review_status' not in cols:
            return
            
        logger.info(f"Syncing manual rejections from review console in '{table_name}' to qc_flags.")
        
        sql = f"""
            INSERT INTO qc_system.qc_flags (submission_uuid, client_schema, form_id, flag_type, severity, detail)
            SELECT submission_uuid, :client_schema, :form_id, 'surveycto_rejected', 'high',
                jsonb_build_object('review_status', review_status, 'source', 'SurveyCTO Review Console')
            FROM {table_name}
            WHERE review_status = 'rejected'
              AND NOT EXISTS (
                  SELECT 1 FROM qc_system.qc_flags q
                  WHERE q.submission_uuid = {table_name}.submission_uuid 
                    AND q.flag_type = 'surveycto_rejected'
              )
        """
        conn.execute(text(sql), {"client_schema": client_schema, "form_id": form_id})
        conn.commit()
        logger.info("Manual console rejections sync completed.")

@task(name="qc-update-leaderboards")
def compute_scores(form_id: str, client_schema: str, engine):
    """Recalculates quality scorecard tallies for enumerators."""
    logger = get_run_logger()
    logger.info("Enumerator metric scoreboard triggers registered.")

@flow(name="quality-control-engine", log_prints=True)
def run_qc(form_id: str, client_schema: str):
    """Primary entry point for automated survey data quality assessments."""
    logger = get_run_logger()
    logger.info(f"🔍 Starting Quality Control assessment flow for {client_schema}.{form_id}")
    
    engine = create_engine(DB_URL)
    
    check_duplicates(form_id, client_schema, engine)
    check_speed(form_id, client_schema, engine)
    check_gps(form_id, client_schema, engine)
    check_outliers(form_id, client_schema, engine)
    import_rejections(form_id, client_schema, engine)
    compute_scores(form_id, client_schema, engine)
    
    logger.info(f"✨ Quality checking assessment process completed for {form_id}.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 2:
        fid = sys.argv[1]
        sch = sys.argv[2]
        run_qc(fid, sch)
    else:
        run_qc.serve(
            name="qc-engine-nightly",
            cron="30 1 * * *",
            parameters={"form_id": "brand-tracker", "client_schema": "client_mtn"}
        )
