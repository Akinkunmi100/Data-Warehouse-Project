-- models/bronze/stg_mtn_project_appraise.sql


with source as (
    select * from "warehouse"."client_mtn"."project_appraise"
),
renamed as (
    select
        -- Standard SurveyCTO fields
        submission_uuid as submission_id,
        "CompletionDate" as completion_date,
        "SubmissionDate" as submission_date,
        starttime as start_time,
        endtime as end_time,
        deviceid as device_id,
        
        -- Default to unknown if missing review_status
        coalesce(review_status, 'unknown') as review_status,
        updated_at as etl_updated_at
        
    from source
)

select * from renamed