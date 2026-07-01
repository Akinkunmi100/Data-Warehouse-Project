{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

select
  initcap(replace(form_id, '_', ' ')) as project_name,
  total_submissions,
  case
    when total_submissions > 0 then 'loaded'
    else 'empty'
  end as data_status,
  case
    when last_run_status = 'failed' and total_submissions > 0 then 'loaded_last_sync_failed'
    when last_run_status = 'failed' then 'failed'
    when last_successful_sync is null then 'not_synced'
    when is_sla_breached then 'stale'
    when last_run_status in ('success', 'success_no_data') then 'healthy'
    else 'attention'
  end as sync_status,
  case
    when total_submissions > 0 and last_run_status = 'failed' then 'loaded - sync needs attention'
    when total_submissions > 0 and is_sla_breached then 'loaded - stale sync'
    when total_submissions > 0 then 'loaded - healthy'
    when last_run_status = 'failed' then 'no data - sync failed'
    when last_successful_sync is null then 'no data - not synced'
    else 'no data'
  end as dashboard_status,
  last_run_status,
  last_successful_sync,
  minutes_since_last_success,
  is_sla_breached,
  total_qc_flags,
  critical_qc_flags,
  high_qc_flags,
  flagged_submission_rate_pct,
  schema_version_count
from {{ ref('executive_overview') }}
