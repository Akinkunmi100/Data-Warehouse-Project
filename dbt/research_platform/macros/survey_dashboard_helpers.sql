{% macro active_survey_forms() %}
  {% set fallback_forms = [
    {"form_id": "project_appraise", "client_schema": "client_mtn", "table_name": "project_appraise"},
    {"form_id": "cis_consumer_june", "client_schema": "client_cis_consumer_june", "table_name": "cis_consumer_june"},
    {"form_id": "construction_sites", "client_schema": "client_construction_sites", "table_name": "construction_sites"},
    {"form_id": "project_ojude_oba", "client_schema": "client_project_ojude_oba", "table_name": "project_ojude_oba"},
    {"form_id": "retail_and_distributor_mortar_may_ending", "client_schema": "client_retail_and_distributor_mortar_may_ending", "table_name": "retail_and_distributor_mortar_may_ending"}
  ] %}

  {% if execute %}
    {% set registry_query %}
      select form_id, client_schema, table_name
      from qc_system.registered_forms
      where active
      order by form_id
    {% endset %}
    {% set registry = run_query(registry_query) %}
    {% if registry is not none and registry.rows | length > 0 %}
      {% set forms = [] %}
      {% for row in registry.rows %}
        {% do forms.append({"form_id": row[0], "client_schema": row[1], "table_name": row[2]}) %}
      {% endfor %}
      {% do return(forms) %}
    {% endif %}
  {% endif %}

  {% do return(fallback_forms) %}
{% endmacro %}

{% macro relation_columns(schema_name, table_name) %}
  {% if not execute %}
    {% do return([]) %}
  {% endif %}
  {% set relation = adapter.get_relation(database=target.database, schema=schema_name, identifier=table_name) %}
  {% if relation is none %}
    {% do return([]) %}
  {% endif %}
  {% do return(adapter.get_columns_in_relation(relation) | map(attribute='name') | list) %}
{% endmacro %}

{% macro has_column(columns, column_name) %}
  {% set lower_cols = columns | map('lower') | list %}
  {% do return((column_name | lower) in lower_cols) %}
{% endmacro %}

{% macro existing_column_name(columns, column_name) %}
  {% for existing in columns %}
    {% if (existing | lower) == (column_name | lower) %}
      {% do return(existing) %}
    {% endif %}
  {% endfor %}
  {% do return(none) %}
{% endmacro %}

{% macro first_existing_column(columns, candidates) %}
  {% for candidate in candidates %}
    {% set existing = existing_column_name(columns, candidate) %}
    {% if existing %}
      {% do return(existing) %}
    {% endif %}
  {% endfor %}
  {% do return(none) %}
{% endmacro %}

{% macro text_value(columns, candidates, fallback="'Unknown'::text") %}
  {% set parts = [] %}
  {% for candidate in candidates %}
    {% set existing = existing_column_name(columns, candidate) %}
    {% if existing %}
      {% do parts.append("nullif(btrim(cast(\"" ~ existing ~ "\" as text)), '')") %}
    {% endif %}
  {% endfor %}
  {% if parts | length > 0 %}
    coalesce({{ parts | join(', ') }}, {{ fallback }})
  {% else %}
    {{ fallback }}
  {% endif %}
{% endmacro %}

{% macro nullable_text_value(columns, candidates) %}
  {% set parts = [] %}
  {% for candidate in candidates %}
    {% set existing = existing_column_name(columns, candidate) %}
    {% if existing %}
      {% do parts.append("nullif(btrim(cast(\"" ~ existing ~ "\" as text)), '')") %}
    {% endif %}
  {% endfor %}
  {% if parts | length > 0 %}
    coalesce({{ parts | join(', ') }})
  {% else %}
    null::text
  {% endif %}
{% endmacro %}

{% macro review_status_value(columns) %}
  {% if has_column(columns, 'review_status') %}
    lower(coalesce(nullif(btrim(cast("review_status" as text)), ''), 'unknown'))
  {% else %}
    'unknown'::text
  {% endif %}
{% endmacro %}

{% macro submission_date_value(columns) %}
  {% set submission_date_col = first_existing_column(columns, ['SubmissionDate', 'submission_date', 'submitted_at', 'SubmissionDateTime']) %}
  {% set timestamp_col = first_existing_column(columns, ['starttime', 'endtime', 'updated_at', 'created_at']) %}
  {% if submission_date_col %}
    case
      when nullif(btrim(cast("{{ submission_date_col }}" as text)), '') ~ '^[A-Za-z]{3} [0-9]{1,2}, [0-9]{4}'
        then to_timestamp(nullif(btrim(cast("{{ submission_date_col }}" as text)), ''), 'Mon DD, YYYY HH12:MI:SS AM')::date
      when nullif(btrim(cast("{{ submission_date_col }}" as text)), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        then nullif(btrim(cast("{{ submission_date_col }}" as text)), '')::timestamptz::date
      else null::date
    end
  {% elif timestamp_col %}
    case
      when nullif(btrim(cast("{{ timestamp_col }}" as text)), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        then nullif(btrim(cast("{{ timestamp_col }}" as text)), '')::timestamptz::date
      else null::date
    end
  {% else %}
    null::date
  {% endif %}
{% endmacro %}

{% macro state_value(form_id, columns) %}
  {% if form_id == 'construction_sites' and has_column(columns, 'sta') %}
    coalesce(nigeria_state_codes.state_label, {{ text_value(columns, ['sta', 'Reg', 'state', 'region']) }})
  {% else %}
    {{ text_value(columns, ['state', 'sta', 'Reg', 'region', 'lga', 'ward']) }}
  {% endif %}
{% endmacro %}

{% macro state_join(form_id, columns) %}
  {% if form_id == 'construction_sites' and has_column(columns, 'sta') %}
    left join nigeria_state_codes
      on nullif(ltrim(btrim(cast("sta" as text)), '0'), '') = nigeria_state_codes.state_code
  {% endif %}
{% endmacro %}

{% macro interviewer_value(columns) %}
  {{ text_value(columns, ['int_name', 'interviewer_id', 'enumerator_id', 'enumeratorid', 'deviceid', 'username'], "'Unknown'::text") }}
{% endmacro %}

{% macro respondent_value(columns) %}
  {% set phone_col = first_existing_column(columns, ['RPN', 'resp_num', 'respondent_phone', 'phone', 'phone_number']) %}
  {% set name_col = first_existing_column(columns, ['RN', 'resp_name', 'respondent_name', 'respondent_id', 'household_id', 'hh_id', 'case_id']) %}
  coalesce(
    {% if phone_col %}
      nullif(regexp_replace(btrim(cast("{{ phone_col }}" as text)), '[^0-9]', '', 'g'), ''),
    {% endif %}
    {% if name_col %}
      nullif(lower(btrim(cast("{{ name_col }}" as text))), ''),
    {% endif %}
    null::text
  )
{% endmacro %}

{% macro submission_uuid_value(columns) %}
  {% if has_column(columns, 'submission_uuid') %}
    cast("submission_uuid" as text)
  {% elif has_column(columns, 'KEY') %}
    cast("KEY" as text)
  {% else %}
    null::text
  {% endif %}
{% endmacro %}
