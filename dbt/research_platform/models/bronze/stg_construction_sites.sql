{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

select * from {{ source('client_construction_sites', 'construction_sites') }}
