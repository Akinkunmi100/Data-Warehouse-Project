{#
  generate_schema_name — overrides dbt's default behaviour.

  Default dbt behaviour: concatenates the target schema (e.g. 'public') with
  a custom schema name to produce 'public_bronze', 'public_silver', etc.

  This macro produces JUST the custom_schema_name when one is set, so the
  dbt_project.yml values (+schema: bronze / silver / gold) are used verbatim.
  Models without a custom schema fall back to the target schema as normal.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
