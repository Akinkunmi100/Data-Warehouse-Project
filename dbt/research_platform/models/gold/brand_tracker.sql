{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer — Brand tracker aggregates
  ---------------------------------------
  Aggregates brand-awareness and brand-preference question responses from
  survey forms. Used as the source for client-facing PDF reports generated
  by the WeasyPrint pipeline.

  This model is intentionally generic — it aggregates any form field whose
  name starts with 'brand_' or contains 'awareness' / 'preference'. Forms
  that do not have these fields will return zero rows (no error).

  Extend this model as the MTN / Unilever form schemas stabilise:
  add explicit column references once you have confirmed column names
  from `make db-shell` → inspect client_mtn.project_appraise.
*/

with mtn_brand as (
    select
        submitted_at::date              as report_date,
        'client_mtn'                    as client_schema,
        'project_appraise'              as form_id,
        count(*)                        as total_responses,
        count(*) filter (where is_approved) as approved_responses,
        -- Placeholder brand metric columns — replace with real field names
        -- after the first ETL run. Example:
        --   avg(brand_awareness_score::numeric)     as avg_awareness_score,
        --   avg(brand_preference_score::numeric)    as avg_preference_score,
        null::numeric                   as avg_awareness_score,
        null::numeric                   as avg_preference_score
    from {{ ref('fct_mtn_survey_responses') }}
    where submitted_at is not null
    group by 1
)

select
    report_date,
    client_schema,
    form_id,
    total_responses,
    approved_responses,
    round(
        approved_responses::numeric
        / nullif(total_responses, 0) * 100
    , 2)                                as approval_rate_pct,
    avg_awareness_score,
    avg_preference_score
from mtn_brand
order by report_date desc
