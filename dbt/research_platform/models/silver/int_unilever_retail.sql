{{
  config(
    materialized = 'view',
    schema       = 'silver'
  )
}}

/*
  Silver layer — Unilever retail census (cleaned)
  -------------------------------------------------
  Normalises review_status and adds approval flags.
  Extend with explicit column casts as the Unilever form schema is confirmed.

  FIX Bug 3: was `SELECT submission_uuid, ..., updated_at, *` — duplicate columns.
  Fix: SELECT * first, then derived columns with non-conflicting names.
*/

with source as (
    select * from {{ ref('stg_unilever_retail') }}
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
