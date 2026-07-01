{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

with quota as (
  select
    project_name,
    sum(coalesce(quota_target, 0)) as quota_target,
    sum(achieved_submissions) as achieved_submissions,
    sum(coalesce(remaining_to_quota, 0)) as remaining_to_quota,
    count(*) filter (where has_quota and achieved_submissions < quota_target) as states_behind_quota
  from {{ ref('fact_powerbi_state_quota') }}
  group by 1
),

duplicates as (
  select
    project_name,
    sum(duplicate_count) as duplicate_count,
    count(distinct respondent_reference) as duplicated_respondents
  from {{ ref('fact_powerbi_duplicates') }}
  group by 1
),

enumerators as (
  select
    project_name,
    count(distinct enumerator_name) as enumerator_count,
    round(avg(quality_score), 2) as avg_quality_score,
    count(*) filter (where quality_band in ('watch', 'needs_review')) as enumerators_needing_review
  from {{ ref('fact_powerbi_enumerator_scorecard') }}
  group by 1
),

regions as (
  select
    project_name,
    count(distinct state_name) as states_with_submissions,
    round(avg(approval_rate_pct), 2) as avg_state_approval_rate,
    count(*) filter (where approval_band in ('watch', 'needs_review')) as states_needing_review
  from {{ ref('fact_powerbi_regional_performance') }}
  group by 1
),

pipeline as (
  select
    project_name,
    pipeline_status,
    is_sla_breached
  from {{ ref('fact_powerbi_pipeline_health') }}
)

select
  p.project_name,
  p.total_submissions,
  p.data_status,
  p.sync_status,
  p.dashboard_status,
  p.total_qc_flags,
  p.critical_qc_flags,
  p.high_qc_flags,
  p.flagged_submission_rate_pct,
  coalesce(q.quota_target, 0) as quota_target,
  coalesce(q.achieved_submissions, 0) as achieved_submissions,
  coalesce(q.remaining_to_quota, 0) as remaining_to_quota,
  coalesce(q.states_behind_quota, 0) as states_behind_quota,
  coalesce(d.duplicate_count, 0) as duplicate_count,
  coalesce(d.duplicated_respondents, 0) as duplicated_respondents,
  coalesce(e.enumerator_count, 0) as enumerator_count,
  e.avg_quality_score,
  coalesce(e.enumerators_needing_review, 0) as enumerators_needing_review,
  coalesce(r.states_with_submissions, 0) as states_with_submissions,
  r.avg_state_approval_rate,
  coalesce(r.states_needing_review, 0) as states_needing_review,
  coalesce(pl.pipeline_status, 'unknown') as pipeline_status,
  coalesce(pl.is_sla_breached, false) as is_sla_breached,
  least(
    100,
    (
      case when coalesce(pl.is_sla_breached, false) then 25 else 0 end
      + case when p.sync_status in ('failed', 'loaded_last_sync_failed') then 20 when p.sync_status = 'stale' then 10 else 0 end
      + case when p.critical_qc_flags > 0 then 20 when p.high_qc_flags > 0 then 10 else 0 end
      + case when coalesce(q.states_behind_quota, 0) > 0 then 15 else 0 end
      + case when coalesce(d.duplicate_count, 0) > 0 then 10 else 0 end
      + case when coalesce(e.enumerators_needing_review, 0) > 0 then 10 else 0 end
    )
  )::integer as operational_risk_score,
  case
    when (
      case when coalesce(pl.is_sla_breached, false) then 25 else 0 end
      + case when p.sync_status in ('failed', 'loaded_last_sync_failed') then 20 when p.sync_status = 'stale' then 10 else 0 end
      + case when p.critical_qc_flags > 0 then 20 when p.high_qc_flags > 0 then 10 else 0 end
      + case when coalesce(q.states_behind_quota, 0) > 0 then 15 else 0 end
      + case when coalesce(d.duplicate_count, 0) > 0 then 10 else 0 end
      + case when coalesce(e.enumerators_needing_review, 0) > 0 then 10 else 0 end
    ) >= 70 then 'critical'
    when (
      case when coalesce(pl.is_sla_breached, false) then 25 else 0 end
      + case when p.sync_status in ('failed', 'loaded_last_sync_failed') then 20 when p.sync_status = 'stale' then 10 else 0 end
      + case when p.critical_qc_flags > 0 then 20 when p.high_qc_flags > 0 then 10 else 0 end
      + case when coalesce(q.states_behind_quota, 0) > 0 then 15 else 0 end
      + case when coalesce(d.duplicate_count, 0) > 0 then 10 else 0 end
      + case when coalesce(e.enumerators_needing_review, 0) > 0 then 10 else 0 end
    ) >= 40 then 'high'
    when (
      case when coalesce(pl.is_sla_breached, false) then 25 else 0 end
      + case when p.sync_status in ('failed', 'loaded_last_sync_failed') then 20 when p.sync_status = 'stale' then 10 else 0 end
      + case when p.critical_qc_flags > 0 then 20 when p.high_qc_flags > 0 then 10 else 0 end
      + case when coalesce(q.states_behind_quota, 0) > 0 then 15 else 0 end
      + case when coalesce(d.duplicate_count, 0) > 0 then 10 else 0 end
      + case when coalesce(e.enumerators_needing_review, 0) > 0 then 10 else 0 end
    ) > 0 then 'watch'
    else 'healthy'
  end as operational_risk_band
from {{ ref('dim_powerbi_project') }} p
left join quota q on q.project_name = p.project_name
left join duplicates d on d.project_name = p.project_name
left join enumerators e on e.project_name = p.project_name
left join regions r on r.project_name = p.project_name
left join pipeline pl on pl.project_name = p.project_name
