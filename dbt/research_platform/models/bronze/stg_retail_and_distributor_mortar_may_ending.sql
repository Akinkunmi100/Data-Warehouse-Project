{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

select * from {{ source('client_retail_and_distributor_mortar_may_ending', 'retail_and_distributor_mortar_may_ending') }}
