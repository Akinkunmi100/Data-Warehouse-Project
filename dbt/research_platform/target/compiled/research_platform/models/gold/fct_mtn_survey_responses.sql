-- models/gold/fct_mtn_survey_responses.sql
-- Fact table: one row per survey submission, with computed duration


with silver as (
    select * from "warehouse"."public_silver"."int_mtn_project_appraise"
)

select
    submission_id,
    device_id,
    review_status,
    completed_at,
    submitted_at,
    started_at,
    ended_at,
    etl_updated_at,

    -- Derived metric: survey completion time in minutes
    case
        when started_at is not null and ended_at is not null
        then extract(epoch from (ended_at - started_at)) / 60.0
        else null
    end as survey_duration_minutes

from silver