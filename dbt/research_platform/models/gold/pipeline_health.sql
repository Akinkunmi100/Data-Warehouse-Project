{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer — Pipeline health & SLA monitoring
  -----------------------------------------------
  Joins sync_state against pipeline_sla to surface:
    • Whether each pipeline has run within its expected window
    • How many minutes ago it last ran
    • An 'is_breached' flag that Metabase can alert on

  Intended for the Metabase "Platform Health" dashboard.
*/

with sla as (
    select
        pipeline_name,
        expected_interval_minutes,
        max_lag_minutes,
        owner
    from {{ source('qc_system', 'pipeline_sla') }}
),

state as (
    select
        pipeline_name,
        last_successful_sync,
        last_run_status,
        updated_at
    from {{ source('qc_system', 'sync_state') }}
),

health as (
    select
        s.pipeline_name,
        s.owner,
        st.last_run_status,
        st.last_successful_sync,
        st.updated_at                               as last_attempted_at,

        -- Minutes since last successful run (null if never run)
        case
            when st.last_successful_sync is not null
            then round(extract(epoch from (now() - st.last_successful_sync)) / 60)
            else null
        end                                         as minutes_since_last_success,

        s.max_lag_minutes,

        -- SLA breach flag: true when lag exceeds the max allowed
        case
            when st.last_successful_sync is null then true
            when extract(epoch from (now() - st.last_successful_sync)) / 60 > s.max_lag_minutes then true
            else false
        end                                         as is_sla_breached

    from sla s
    left join state st using (pipeline_name)
)

select * from health
order by is_sla_breached desc, minutes_since_last_success desc nulls first
