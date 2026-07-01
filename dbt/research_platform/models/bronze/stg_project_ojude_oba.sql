{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

select * from {{ source('client_project_ojude_oba', 'project_ojude_oba') }}
