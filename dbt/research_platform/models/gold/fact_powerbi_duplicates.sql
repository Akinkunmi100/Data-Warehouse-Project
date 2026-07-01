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
  enumerator_name,
  enumerator_count,
  case
    when respondent_id is null then 'Unknown'
    when length(respondent_id) <= 4 then repeat('*', length(respondent_id))
    else left(respondent_id, 3) || repeat('*', greatest(length(respondent_id) - 5, 1)) || right(respondent_id, 2)
  end as respondent_reference,
  submission_count,
  duplicate_count,
  case
    when duplicate_count >= 5 then 'critical'
    when duplicate_count >= 3 then 'high'
    else 'medium'
  end as duplicate_severity
from {{ ref('duplicates_by_state_respondent') }}
