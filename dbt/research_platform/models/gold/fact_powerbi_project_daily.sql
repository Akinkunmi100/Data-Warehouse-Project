{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

select
  initcap(replace(form_id, '_', ' ')) as project_name,
  submission_date,
  total_submissions,
  approved_submissions,
  rejected_submissions,
  pending_or_unknown_submissions
from {{ ref('project_daily_submissions') }}
