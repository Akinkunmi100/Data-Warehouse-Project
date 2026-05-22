
  create view "warehouse"."public_silver"."int_mtn_project_appraise__dbt_tmp"
    
    
  as (
    -- models/silver/int_mtn_project_appraise.sql
-- Cleansed layer: cast types, standardise column names


with bronze as (
    select * from "warehouse"."public_bronze"."stg_mtn_project_appraise"
)

select
    submission_id,
    device_id,
    review_status,

    -- Safe timestamp casts (return NULL on malformed values via NULLIF)
    case when completion_date is not null and completion_date <> ''
         then completion_date::timestamp else null end as completed_at,
    case when submission_date is not null and submission_date <> ''
         then submission_date::timestamp else null end as submitted_at,
    case when start_time is not null and start_time <> ''
         then start_time::timestamp else null end      as started_at,
    case when end_time is not null and end_time <> ''
         then end_time::timestamp else null end        as ended_at,

    etl_updated_at

from bronze
  );