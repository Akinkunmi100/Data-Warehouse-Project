{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer - Pipeline health and SLA monitoring
  -----------------------------------------------
  Registry-driven health for active SurveyCTO form pipelines.
*/

with forms as (
    select
        form_id,
        client,
        client_schema,
        etl_deployment,
        qc_deployment
    from {{ source('qc_system', 'registered_forms') }}
    where active
),

state as (
    select
        pipeline_name,
        last_successful_sync,
        last_run_status,
        updated_at as last_attempted_at,
        case
            when last_run_status = 'success_no_data' then updated_at
            else last_successful_sync
        end as effective_success_at
    from {{ source('qc_system', 'sync_state') }}
),

health as (
    select
        f.form_id as pipeline_name,
        'data_team'::text as owner,
        f.client,
        f.client_schema,
        f.etl_deployment,
        f.qc_deployment,
        st.last_run_status,
        st.last_successful_sync,
        st.last_attempted_at,
        case
            when st.effective_success_at is not null
            then round(extract(epoch from (now() - st.effective_success_at)) / 60)
            else null
        end as minutes_since_last_success,
        1440::integer as max_lag_minutes,
        case
            when st.effective_success_at is null then true
            when extract(epoch from (now() - st.effective_success_at)) / 60 > 1440 then true
            else false
        end as is_sla_breached
    from forms f
    left join state st
        on st.pipeline_name = f.form_id
)

select * from health
order by is_sla_breached desc, minutes_since_last_success desc nulls first
