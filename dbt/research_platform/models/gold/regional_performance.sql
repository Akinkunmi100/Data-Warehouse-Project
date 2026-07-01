{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Live regional/state performance across every active project represented in
  achievements_by_state_respondent.
*/

select
    state_name as region_name,
    client_schema,
    form_id,
    sum(total_submissions) as total_submissions,
    sum(approved_submissions) as approved_submissions,
    sum(rejected_submissions) as rejected_submissions,
    round(
        sum(approved_submissions)::numeric
        / nullif(sum(total_submissions), 0) * 100
    , 2) as approval_rate_pct,
    null::numeric as avg_duration_minutes,
    rank() over (
        partition by client_schema, form_id
        order by sum(total_submissions) desc
    ) as rank_by_volume
from {{ ref('achievements_by_state_respondent') }}
group by 1, 2, 3
order by client_schema, form_id, rank_by_volume
