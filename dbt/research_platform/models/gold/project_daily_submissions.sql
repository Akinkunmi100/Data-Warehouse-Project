{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer - Daily submissions by project
  -----------------------------------------
  Dashboard-ready daily volume across all active SurveyCTO forms known to the
  dbt dashboard helper registry.
*/

with all_submissions as (
  {% for form in active_survey_forms() %}
    {% set columns = relation_columns(form.client_schema, form.table_name) %}
    {% if columns | length > 0 %}
      select
        '{{ form.form_id }}'::text as form_id,
        '{{ form.client_schema }}'::text as client_schema,
        {{ submission_date_value(columns) }} as submission_date,
        {{ review_status_value(columns) }} as review_status
      from {{ form.client_schema }}.{{ form.table_name }}
      {% if not loop.last %}union all{% endif %}
    {% else %}
      select
        '{{ form.form_id }}'::text as form_id,
        '{{ form.client_schema }}'::text as client_schema,
        null::date as submission_date,
        'unknown'::text as review_status
      where false
      {% if not loop.last %}union all{% endif %}
    {% endif %}
  {% endfor %}
)

select
  form_id,
  client_schema,
  submission_date,
  count(*) as total_submissions,
  count(*) filter (where review_status = 'approved') as approved_submissions,
  count(*) filter (where review_status = 'rejected') as rejected_submissions,
  count(*) filter (where review_status not in ('approved', 'rejected')) as pending_or_unknown_submissions
from all_submissions
where submission_date is not null
group by 1, 2, 3
order by form_id, submission_date
