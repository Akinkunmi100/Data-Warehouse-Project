{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

select
  initcap(replace(form_id, '_', ' ')) as project_name,
  flag_date,
  flag_type,
  severity,
  flag_count,
  affected_submissions,
  rolling_7d_count
from {{ ref('qc_flag_summary') }}
