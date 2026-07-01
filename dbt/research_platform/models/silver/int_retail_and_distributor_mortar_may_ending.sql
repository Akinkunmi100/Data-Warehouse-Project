{{
  config(
    materialized = 'view',
    schema       = 'silver'
  )
}}

with source as (
  select * from {{ ref('stg_retail_and_distributor_mortar_may_ending') }}
)

select
  *,
  case
    when lower(review_status) in ('approved', 'rejected') then lower(review_status)
    else null
  end as review_status_clean,
  (lower(review_status) = 'approved') as is_approved,
  (lower(review_status) = 'rejected') as is_rejected
from source
