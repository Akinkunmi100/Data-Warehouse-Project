{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

with project_summary as (
  select
    count(distinct project_name)::integer as active_projects,
    count(distinct project_name) filter (where total_submissions > 0)::integer as loaded_projects,
    coalesce(sum(total_qc_flags), 0)::integer as total_qc_flags,
    coalesce(sum(critical_qc_flags), 0)::integer as critical_qc_flags,
    coalesce(sum(high_qc_flags), 0)::integer as high_qc_flags
from {{ ref('dim_powerbi_project') }}
),

quota_summary as (
  select
    coalesce(sum(quota_target), 0)::integer as total_quota,
    coalesce(sum(achieved_submissions), 0)::integer as achieved_submissions,
    coalesce(sum(approved_submissions), 0)::integer as approved_submissions,
    coalesce(sum(rejected_submissions), 0)::integer as rejected_submissions,
    coalesce(sum(pending_or_unknown_submissions), 0)::integer as pending_or_unknown_submissions,
    round(
      coalesce(sum(achieved_submissions), 0)::numeric
      / nullif(sum(quota_target), 0) * 100,
      2
    ) as achievement_rate_pct,
    coalesce(sum(remaining_to_quota), 0)::integer as remaining_to_quota,
    count(*) filter (where has_quota and achieved_submissions < quota_target)::integer as states_behind_quota
  from {{ ref('fact_powerbi_state_quota') }}
),

risk_summary as (
  select
    coalesce(sum(duplicate_count), 0)::integer as duplicate_count,
    coalesce(sum(enumerators_needing_review), 0)::integer as enumerators_needing_review,
    coalesce(sum(states_needing_review), 0)::integer as states_needing_review,
    coalesce(max(operational_risk_score), 0)::integer as max_project_risk_score,
    count(*) filter (where operational_risk_band in ('critical', 'high'))::integer as high_risk_projects
  from {{ ref('powerbi_project_risk_summary') }}
)

select
  current_timestamp as snapshot_at,
  q.total_quota,
  q.achieved_submissions,
  q.approved_submissions,
  q.rejected_submissions,
  q.pending_or_unknown_submissions,
  q.achievement_rate_pct,
  q.remaining_to_quota,
  q.states_behind_quota,
  p.active_projects,
  p.loaded_projects,
  p.total_qc_flags,
  p.critical_qc_flags,
  p.high_qc_flags,
  r.duplicate_count,
  r.enumerators_needing_review,
  r.states_needing_review,
  r.max_project_risk_score,
  r.high_risk_projects
from project_summary p
cross join quota_summary q
cross join risk_summary r
