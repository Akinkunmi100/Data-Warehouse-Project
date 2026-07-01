{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer - Duplicate respondents by state
  -------------------------------------------
  Repeated respondent identifiers within the same state/form. Phone-like
  fields are preferred, then respondent name/id fields.
*/

with nigeria_state_codes(state_code, state_label) as (
  values
    ('1', 'Abia'), ('2', 'Adamawa'), ('3', 'Akwa Ibom'),
    ('4', 'Anambra'), ('5', 'Bauchi'), ('6', 'Bayelsa'),
    ('7', 'Benue'), ('8', 'Borno'), ('9', 'Cross River'),
    ('10', 'Delta'), ('11', 'Ebonyi'), ('12', 'Edo'),
    ('13', 'Ekiti'), ('14', 'Enugu'), ('15', 'FCT Abuja'),
    ('16', 'Gombe'), ('17', 'Imo'), ('18', 'Jigawa'),
    ('19', 'Kaduna'), ('20', 'Kano'), ('21', 'Katsina'),
    ('22', 'Kebbi'), ('23', 'Kogi'), ('24', 'Kwara'),
    ('25', 'Lagos'), ('26', 'Nasarawa'), ('27', 'Niger'),
    ('28', 'Ogun'), ('29', 'Ondo'), ('30', 'Osun'),
    ('31', 'Oyo'), ('32', 'Plateau'), ('33', 'Rivers'),
    ('34', 'Sokoto'), ('35', 'Taraba'), ('36', 'Yobe'),
    ('37', 'Zamfara')
),

source_rows as (
  {% for form in active_survey_forms() %}
    {% set columns = relation_columns(form.client_schema, form.table_name) %}
    {% if columns | length > 0 %}
      select
        '{{ form.form_id }}'::text as form_id,
        '{{ form.client_schema }}'::text as client_schema,
        {{ state_value(form.form_id, columns) }} as state_name,
        {{ interviewer_value(columns) }} as enumerator_name,
        {{ respondent_value(columns) }} as respondent_id
      from {{ form.client_schema }}.{{ form.table_name }}
      {{ state_join(form.form_id, columns) }}
      {% if not loop.last %}union all{% endif %}
    {% else %}
      select
        '{{ form.form_id }}'::text as form_id,
        '{{ form.client_schema }}'::text as client_schema,
        'Unknown'::text as state_name,
        'Unknown'::text as enumerator_name,
        null::text as respondent_id
      where false
      {% if not loop.last %}union all{% endif %}
    {% endif %}
  {% endfor %}
),

duplicates as (
  select
    form_id,
    client_schema,
    state_name,
    respondent_id,
    string_agg(distinct enumerator_name, '; ' order by enumerator_name) as enumerator_name,
    count(distinct enumerator_name) as enumerator_count,
    count(*) as submission_count
  from source_rows
  where respondent_id is not null
    and respondent_id not in (
      '0', '00', '0000000000', '00000000000', '08000000000',
      'none', 'no', 'nil', 'na', 'n/a', 'unknown'
    )
  group by 1, 2, 3, 4
)

select
  form_id,
  client_schema,
  state_name,
  enumerator_name,
  enumerator_count,
  respondent_id,
  submission_count,
  submission_count - 1 as duplicate_count
from duplicates
where submission_count > 1
order by duplicate_count desc, form_id, state_name, respondent_id
