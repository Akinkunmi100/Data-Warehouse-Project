{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

select
  initcap(replace(pipeline_name, '_', ' ')) as project_name,
  last_run_status,
  last_successful_sync,
  last_attempted_at,
  minutes_since_last_success,
  max_lag_minutes,
  is_sla_breached,
  case
    when last_run_status = 'failed' then 'failed'
    when is_sla_breached then 'stale'
    when last_run_status in ('success', 'success_no_data') then 'healthy'
    when last_successful_sync is null then 'not_synced'
    else 'attention'
  end as pipeline_status
from {{ ref('pipeline_health') }}
