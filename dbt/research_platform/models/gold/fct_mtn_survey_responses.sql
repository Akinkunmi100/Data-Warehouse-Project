{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer — MTN project_appraise fact table
  ---------------------------------------------
  One row per survey submission. Derives interview duration in minutes.

  Column mapping:
    submission_uuid  → ETL guarantee (renamed from KEY)
    updated_at       → ETL guarantee
    deviceid         → SurveyCTO standard field
    starttime        → SurveyCTO standard field
    endtime          → SurveyCTO standard field
    "SubmissionDate" → SurveyCTO wide-schema export column
    "CompletionDate" → SurveyCTO wide-schema export column

  FIX: replaced try_cast() calls — that function does NOT exist in PostgreSQL
  (it is a SQL Server / DuckDB function). Safe PostgreSQL pattern used instead:
  NULLIF(col, '')::timestamptz silently produces NULL for empty strings rather
  than raising an error, which is the correct behaviour for optional time fields.

  NOTE: SurveyCTO preserves exact field names from the XLS form definition.
  If the MTN form uses different casing, inspect client_mtn.project_appraise
  after the first ETL run and update the column names below accordingly.
*/

with silver as (
    select * from {{ ref('int_mtn_project_appraise') }}
)

select
    -- Guaranteed columns (ETL always produces these)
    submission_uuid,
    review_status,
    review_status_clean,
    is_approved,
    is_rejected,
    updated_at,

    -- SurveyCTO standard fields (present in every wide-schema export)
    deviceid                                            as device_id,

    -- Time fields: NULLIF strips empty strings before casting → NULL not error.
    -- try_cast() is not a PostgreSQL function; NULLIF(col,'')::type is the safe form.
    NULLIF("SubmissionDate", '')::timestamptz           as submitted_at,
    NULLIF("CompletionDate", '')::timestamptz           as completed_at,
    NULLIF(starttime,        '')::timestamptz           as started_at,
    NULLIF(endtime,          '')::timestamptz           as ended_at,

    -- Derived metric: survey completion time in minutes
    case
        when NULLIF(starttime, '') is not null
         and NULLIF(endtime,   '') is not null
        then extract(epoch from (
                NULLIF(endtime,   '')::timestamptz
              - NULLIF(starttime, '')::timestamptz
             )) / 60.0
        else null
    end                                                 as survey_duration_minutes

from silver
