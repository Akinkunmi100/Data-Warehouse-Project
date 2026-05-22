{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

/*
  Bronze layer — MTN project_appraise
  ------------------------------------
  Pure pass-through view over the raw warehouse table populated by sync_surveycto.py.
  No transformations; this layer provides a stable dbt reference point so that silver
  models can be rebuilt without touching the source schema.

  FIX Bug 3: was `SELECT submission_uuid, review_status, updated_at, *` which caused
  PostgreSQL to emit those three columns twice (once explicitly, once via *).
  dbt fails or produces a broken view when column names are duplicated.
  Fix: plain SELECT * — bronze is a pure pass-through by design.
*/

select * from {{ source('client_mtn', 'project_appraise') }}
