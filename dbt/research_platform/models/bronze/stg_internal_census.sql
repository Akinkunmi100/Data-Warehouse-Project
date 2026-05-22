{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

/*
  Bronze layer — Internal census
  --------------------------------
  Pure pass-through view over the raw warehouse table.

  FIX Bug 3: was `SELECT submission_uuid, review_status, updated_at, *` — duplicate columns.
  Fix: plain SELECT *.
*/

select * from {{ source('internal', 'internal_census') }}
