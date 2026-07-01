{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  User-facing enumerator dimension.
  Maps technical SurveyCTO device/user keys to the human interviewer name when
  the form provides one.
*/

with enumerator_rows as (
  {% for form in active_survey_forms() %}
    {% set columns = relation_columns(form.client_schema, form.table_name) %}
    {% if columns | length > 0 %}
      select
        initcap(replace('{{ form.form_id }}', '_', ' ')) as project_name,
        {{ text_value(columns, ['enumerator_id', 'enumeratorid', 'deviceid', 'username'], "'Unknown'::text") }} as enumerator_key,
        {{ text_value(columns, ['int_name', 'interviewer_name', 'enumerator_name'], "'Unknown'::text") }} as enumerator_name
      from {{ form.client_schema }}.{{ form.table_name }}
      {% if not loop.last %}union all{% endif %}
    {% else %}
      select
        initcap(replace('{{ form.form_id }}', '_', ' ')) as project_name,
        'Unknown'::text as enumerator_key,
        'Unknown'::text as enumerator_name
      where false
      {% if not loop.last %}union all{% endif %}
    {% endif %}
  {% endfor %}
),

ranked as (
  select
    project_name,
    enumerator_key,
    enumerator_name,
    count(*) as submission_count,
    row_number() over (
      partition by project_name, enumerator_key
      order by
        case when enumerator_name = 'Unknown' then 1 else 0 end,
        count(*) desc,
        enumerator_name
    ) as name_rank
  from enumerator_rows
  where enumerator_key is not null
  group by 1, 2, 3
)

select
  project_name,
  enumerator_key,
  enumerator_name,
  submission_count
from ranked
where name_rank = 1
