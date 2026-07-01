{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer - Achievements by state and interviewer
  --------------------------------------------------
  Counts submissions by state and interviewer across active SurveyCTO forms.
  Form-specific field differences are handled by dashboard helper macros.
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

all_submissions as (
  {% for form in active_survey_forms() %}
    {% set columns = relation_columns(form.client_schema, form.table_name) %}
    {% if columns | length > 0 %}
      select
        '{{ form.form_id }}'::text as form_id,
        '{{ form.client_schema }}'::text as client_schema,
        {{ submission_uuid_value(columns) }} as submission_uuid,
        {{ submission_date_value(columns) }} as submission_date,
        {{ review_status_value(columns) }} as review_status,
        {{ state_value(form.form_id, columns) }} as state_name,
        {{ interviewer_value(columns) }} as interviewer_id,
        {{ nullable_text_value(columns, ['state', 'sta', 'Reg', 'region', 'lga', 'ward']) }} is null as state_missing,
        {{ nullable_text_value(columns, ['int_name', 'interviewer_id', 'enumerator_id', 'enumeratorid', 'deviceid', 'username']) }} is null as interviewer_missing
      from {{ form.client_schema }}.{{ form.table_name }}
      {{ state_join(form.form_id, columns) }}
      {% if not loop.last %}union all{% endif %}
    {% else %}
      select
        '{{ form.form_id }}'::text as form_id,
        '{{ form.client_schema }}'::text as client_schema,
        null::text as submission_uuid,
        null::date as submission_date,
        'unknown'::text as review_status,
        'Unknown'::text as state_name,
        'Unknown'::text as interviewer_id,
        true as state_missing,
        true as interviewer_missing
      where false
      {% if not loop.last %}union all{% endif %}
    {% endif %}
  {% endfor %}
)

select
  form_id,
  client_schema,
  state_name,
  interviewer_id,
  count(*) as total_submissions,
  count(*) filter (where review_status = 'approved') as approved_submissions,
  count(*) filter (where review_status = 'rejected') as rejected_submissions,
  count(*) filter (where review_status not in ('approved', 'rejected')) as pending_or_unknown_submissions,
  count(*) filter (where state_missing) as missing_state_count,
  count(*) filter (where interviewer_missing) as missing_interviewer_count,
  min(submission_date) as first_submission_date,
  max(submission_date) as latest_submission_date
from all_submissions
group by 1, 2, 3, 4
order by total_submissions desc, form_id, state_name, interviewer_id
