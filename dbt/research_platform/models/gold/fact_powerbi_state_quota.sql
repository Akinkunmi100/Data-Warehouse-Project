{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

select
  initcap(replace(form_id, '_', ' ')) as project_name,
  lower(regexp_replace(btrim(state_name), '\s+', ' ', 'g')) as state_key,
  state_name,
  wave_name,
  quota_target,
  achieved_submissions,
  approved_submissions,
  rejected_submissions,
  pending_or_unknown_submissions,
  achievement_rate_pct,
  remaining_to_quota,
  quota_status,
  has_quota,
  latest_submission_date,
  quota_notes
from {{ ref('quota_vs_achievement_by_state') }}
