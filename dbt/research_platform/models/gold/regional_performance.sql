{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer — Regional performance summary
  ------------------------------------------
  Aggregates submission counts, approval rates, and quality metrics by region.
  Feeds the Superset "Field Operations" dashboard.

  Source: silver layer for all three forms (region column required on source forms).
  Falls back gracefully — rows where region is null are grouped as 'Unknown'.
*/

with mtn as (
    select
        coalesce(region, 'Unknown')     as region_name,
        'client_mtn'                    as client_schema,
        'project_appraise'              as form_id,
        count(*)                        as total_submissions,
        count(*) filter (where is_approved)     as approved_submissions,
        count(*) filter (where is_rejected)     as rejected_submissions,
        round(
            count(*) filter (where is_approved)::numeric
            / nullif(count(*), 0) * 100
        , 2)                            as approval_rate_pct,
        avg(
            case when duration_seconds is not null
            then duration_seconds::numeric / 60.0 end
        )                               as avg_duration_minutes
    from {{ ref('int_mtn_project_appraise') }}
    group by 1
),

all_regions as (
    select * from mtn
    -- Add additional client UNION ALL blocks here as more forms go live:
    -- UNION ALL
    -- select ... from {{ ref('int_unilever_retail') }}
)

select
    region_name,
    client_schema,
    form_id,
    total_submissions,
    approved_submissions,
    rejected_submissions,
    approval_rate_pct,
    round(avg_duration_minutes::numeric, 2)     as avg_duration_minutes,
    -- Rank regions by total volume within each client+form
    rank() over (
        partition by client_schema, form_id
        order by total_submissions desc
    )                                           as rank_by_volume
from all_regions
order by client_schema, form_id, rank_by_volume
