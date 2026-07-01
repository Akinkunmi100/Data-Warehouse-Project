{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

select
  initcap(replace(s.form_id, '_', ' ')) as project_name,
  coalesce(nullif(e.enumerator_name, 'Unknown'), 'Unknown Enumerator') as enumerator_name,
  s.total_submissions,
  s.total_flags,
  s.critical_flags,
  s.high_flags,
  s.medium_flags,
  s.quality_score,
  s.rank_in_form,
  s.flag_rate,
  case
    when s.quality_score is null then 'unscored'
    when s.quality_score >= 90 then 'excellent'
    when s.quality_score >= 75 then 'good'
    when s.quality_score >= 60 then 'watch'
    else 'needs_review'
  end as quality_band
from {{ ref('enumerator_scorecard') }} s
left join {{ ref('dim_powerbi_enumerator') }} e
  on e.project_name = initcap(replace(s.form_id, '_', ' '))
 and e.enumerator_key = s.enumerator_id
