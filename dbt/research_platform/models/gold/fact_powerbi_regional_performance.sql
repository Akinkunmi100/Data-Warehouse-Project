{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

select
  initcap(replace(form_id, '_', ' ')) as project_name,
  lower(regexp_replace(btrim(region_name), '\s+', ' ', 'g')) as state_key,
  region_name as state_name,
  total_submissions,
  approved_submissions,
  rejected_submissions,
  approval_rate_pct,
  rank_by_volume,
  case
    when approval_rate_pct is null then 'unknown'
    when approval_rate_pct >= 90 then 'excellent'
    when approval_rate_pct >= 75 then 'good'
    when approval_rate_pct >= 60 then 'watch'
    else 'needs_review'
  end as approval_band
from {{ ref('regional_performance') }}
