{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer — Unilever retail census fact table
  -----------------------------------------------
  One row per survey submission. Mirrors the structure of fct_mtn_survey_responses
  for consistency across clients. The column mapping below follows the SurveyCTO
  wide-schema export convention — adjust if the Unilever form uses different names.

  Standard SurveyCTO metadata fields present in every wide-schema export:
    KEY               → submission_uuid  (renamed by ETL)
    "SubmissionDate"  → submitted_at
    "CompletionDate"  → completed_at
    starttime         → started_at
    endtime           → ended_at
    deviceid          → device_id
    updated_at        (added by ETL, always present)
*/

with silver as (
    select * from {{ ref('int_unilever_retail') }}
)

select
    submission_uuid,
    review_status,
    review_status_clean,
    is_approved,
    is_rejected,
    updated_at,

    deviceid                                        as device_id,

    NULLIF("SubmissionDate", '')::timestamptz       as submitted_at,
    NULLIF("CompletionDate", '')::timestamptz       as completed_at,
    NULLIF(starttime,        '')::timestamptz       as started_at,
    NULLIF(endtime,          '')::timestamptz       as ended_at,

    case
        when NULLIF(starttime, '') is not null
         and NULLIF(endtime,   '') is not null
        then extract(epoch from (
            NULLIF(endtime,   '')::timestamptz
          - NULLIF(starttime, '')::timestamptz
        )) / 60.0
        else null
    end                                             as survey_duration_minutes

from silver
