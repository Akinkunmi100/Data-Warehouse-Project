{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer - Executive overview
  -------------------------------
  One row per registered SurveyCTO form. This is the primary source table for
  a cross-project executive dashboard in Metabase.
*/

with forms as (
    select
        form_id,
        client,
        client_schema,
        table_name,
        active,
        etl_deployment,
        qc_deployment,
        registered_at,
        updated_at as registry_updated_at
    from {{ source('qc_system', 'registered_forms') }}
    where active
),

sync_state as (
    select
        pipeline_name as form_id,
        last_successful_sync,
        last_run_status,
        updated_at as last_attempted_at,
        case
            when last_run_status = 'success_no_data' then updated_at
            else last_successful_sync
        end as effective_success_at
    from {{ source('qc_system', 'sync_state') }}
),

flags as (
    select
        form_id,
        client_schema,
        count(*) as total_qc_flags,
        count(*) filter (where severity = 'critical') as critical_qc_flags,
        count(*) filter (where severity = 'high') as high_qc_flags,
        count(*) filter (where flagged_at >= now() - interval '7 days') as qc_flags_last_7d,
        count(distinct submission_uuid) as flagged_submissions
    from {{ source('qc_system', 'qc_flags') }}
    group by 1, 2
),

schema_versions as (
    select
        form_id,
        max(detected_at) as schema_last_seen_at,
        count(distinct version_hash) as schema_version_count
    from {{ source('qc_system', 'form_versions') }}
    group by 1
),

sla as (
    select
        pipeline_name as form_id,
        owner,
        max_lag_minutes
    from {{ source('qc_system', 'pipeline_sla') }}
)

select
    f.form_id,
    f.client,
    f.client_schema,
    f.table_name,
    f.etl_deployment,
    f.qc_deployment,
    f.registered_at,
    f.registry_updated_at,

    qc_system.table_row_count(f.client_schema, f.table_name) as total_submissions,

    ss.last_run_status,
    ss.last_successful_sync,
    ss.last_attempted_at,
    case
        when ss.effective_success_at is not null
        then round(extract(epoch from (now() - ss.effective_success_at)) / 60)
        else null
    end as minutes_since_last_success,

    coalesce(s.max_lag_minutes, 1440) as max_lag_minutes,
    case
        when ss.effective_success_at is null then true
        when extract(epoch from (now() - ss.effective_success_at)) / 60 > coalesce(s.max_lag_minutes, 1440) then true
        else false
    end as is_sla_breached,

    coalesce(fl.total_qc_flags, 0) as total_qc_flags,
    coalesce(fl.critical_qc_flags, 0) as critical_qc_flags,
    coalesce(fl.high_qc_flags, 0) as high_qc_flags,
    coalesce(fl.qc_flags_last_7d, 0) as qc_flags_last_7d,
    coalesce(fl.flagged_submissions, 0) as flagged_submissions,
    round(
        coalesce(fl.flagged_submissions, 0)::numeric
        / nullif(qc_system.table_row_count(f.client_schema, f.table_name), 0)
        * 100,
        2
    ) as flagged_submission_rate_pct,

    sv.schema_last_seen_at,
    coalesce(sv.schema_version_count, 0) as schema_version_count,

    case
        when ss.last_run_status in ('success', 'success_no_data') and not (
            case
                when ss.effective_success_at is null then true
                when extract(epoch from (now() - ss.effective_success_at)) / 60 > coalesce(s.max_lag_minutes, 1440) then true
                else false
            end
        ) and qc_system.table_row_count(f.client_schema, f.table_name) = 0 then 'healthy_empty'
        when ss.last_run_status in ('success', 'success_no_data') and not (
            case
                when ss.effective_success_at is null then true
                when extract(epoch from (now() - ss.effective_success_at)) / 60 > coalesce(s.max_lag_minutes, 1440) then true
                else false
            end
        ) then 'healthy'
        when ss.last_run_status = 'failed' then 'failed'
        when ss.last_successful_sync is null then 'not_synced'
        else 'attention'
    end as executive_status

from forms f
left join sync_state ss
    on ss.form_id = f.form_id
left join flags fl
    on fl.form_id = f.form_id
    and fl.client_schema = f.client_schema
left join schema_versions sv
    on sv.form_id = f.form_id
left join sla s
    on s.form_id = f.form_id
order by executive_status, total_submissions desc, f.form_id
