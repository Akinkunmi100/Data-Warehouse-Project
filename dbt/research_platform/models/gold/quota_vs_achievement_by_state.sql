{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer - Quota vs achievement by state
  ------------------------------------------
  Compares configured project/state quota targets with actual submissions.
*/

with state_achievement as (
  select
    form_id,
    client_schema,
    state_name,
    sum(total_submissions)::integer as achieved_submissions,
    sum(approved_submissions)::integer as approved_submissions,
    sum(rejected_submissions)::integer as rejected_submissions,
    sum(pending_or_unknown_submissions)::integer as pending_or_unknown_submissions,
    min(first_submission_date) as first_submission_date,
    max(latest_submission_date) as latest_submission_date
  from {{ ref('achievements_by_state_respondent') }}
  group by 1, 2, 3
),

active_quotas as (
  select
    form_id,
    client_schema,
    state_name,
    wave_name,
    sum(quota_target)::integer as quota_target,
    string_agg(nullif(notes, ''), '; ' order by notes) filter (where nullif(notes, '') is not null) as quota_notes
  from {{ source('qc_system', 'project_state_quotas') }}
  where active
  group by 1, 2, 3, 4
)

select
  coalesce(q.form_id, a.form_id) as form_id,
  coalesce(q.client_schema, a.client_schema) as client_schema,
  coalesce(q.state_name, a.state_name) as state_name,
  coalesce(q.wave_name, 'default') as wave_name,
  q.quota_target,
  coalesce(a.achieved_submissions, 0) as achieved_submissions,
  coalesce(a.approved_submissions, 0) as approved_submissions,
  coalesce(a.rejected_submissions, 0) as rejected_submissions,
  coalesce(a.pending_or_unknown_submissions, 0) as pending_or_unknown_submissions,
  case
    when q.quota_target is null or q.quota_target = 0 then null
    else round((coalesce(a.achieved_submissions, 0)::numeric / q.quota_target) * 100, 2)
  end as achievement_rate_pct,
  case
    when q.quota_target is null then null
    else greatest(q.quota_target - coalesce(a.achieved_submissions, 0), 0)
  end as remaining_to_quota,
  case
    when q.quota_target is null then 'no_quota'
    when coalesce(a.achieved_submissions, 0) = 0 then 'not_started'
    when coalesce(a.achieved_submissions, 0) >= q.quota_target then 'met'
    when coalesce(a.achieved_submissions, 0)::numeric / nullif(q.quota_target, 0) >= 0.8 then 'on_track'
    else 'behind'
  end as quota_status,
  (q.quota_target is not null) as has_quota,
  (a.form_id is not null) as has_achievement,
  a.first_submission_date,
  a.latest_submission_date,
  q.quota_notes
from active_quotas q
full outer join state_achievement a
  on a.form_id = q.form_id
 and a.client_schema = q.client_schema
 and lower(trim(a.state_name)) = lower(trim(q.state_name))
order by form_id, wave_name, remaining_to_quota desc nulls last, achieved_submissions desc
