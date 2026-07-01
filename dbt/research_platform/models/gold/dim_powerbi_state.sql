{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

with states as (
  select distinct state_name from {{ ref('quota_vs_achievement_by_state') }}
  union
  select distinct region_name as state_name from {{ ref('regional_performance') }}
),

normalized as (
  select
    lower(regexp_replace(btrim(state_name), '\s+', ' ', 'g')) as state_key,
    initcap(regexp_replace(btrim(state_name), '\s+', ' ', 'g')) as state_name
  from states
  where state_name is not null
)

select
  state_key,
  min(state_name) as state_name,
  case
    when state_key in ('unknown', 'no quota loaded', 'no quota or submissions yet') then false
    else true
  end as is_real_state
from normalized
group by state_key
