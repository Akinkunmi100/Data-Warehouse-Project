{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

select * from {{ source('client_cis_consumer_june', 'cis_consumer_june') }}
