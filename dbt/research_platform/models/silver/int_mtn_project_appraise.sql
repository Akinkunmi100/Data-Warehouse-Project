{{
  config(
    materialized = 'view',
    schema       = 'silver'
  )
}}

/*
  Silver layer — MTN project_appraise (cleaned)
  -----------------------------------------------
  Adds:
    • review_status_clean: lower-cased review status; values outside
      ('approved', 'rejected') are coerced to NULL
    • is_approved / is_rejected: boolean convenience flags for downstream gold models
    • All dynamic form columns passed through unchanged via SELECT *

  FIX Bug 3: previous version listed `submission_uuid, review_status, updated_at`
  explicitly before `*`, which emitted those columns twice (duplicate column names).
  PostgreSQL will reject a CREATE VIEW with duplicate column names.
  Fix: SELECT * first, then append derived columns with non-conflicting names.
  The raw `review_status` column (already lowercase from SurveyCTO) is preserved
  for backward-compatibility with gold models that filter on it directly.

  TODO: as the form schema stabilises, add explicit CAST columns here
  (e.g. submission_date::date, duration_seconds::int, etc.)
*/

with source as (
    select * from {{ ref('stg_mtn_project_appraise') }}
),

cleaned as (
    select
        *,

        -- Normalised review status: lowercase; non-standard values → null
        case
            when lower(review_status) in ('approved', 'rejected') then lower(review_status)
            else null
        end                                         as review_status_clean,

        (lower(review_status) = 'approved')         as is_approved,
        (lower(review_status) = 'rejected')         as is_rejected

    from source
)

select * from cleaned
