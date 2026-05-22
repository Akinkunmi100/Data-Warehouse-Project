{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer — MTN daily submission aggregates
  ---------------------------------------------
  One row per calendar day. Feeds the Metabase "Daily Submissions" dashboard.

  FIX: was referencing submitted_at directly from the silver layer. That column
  doesn't exist there — it's produced by fct_mtn_survey_responses via a CAST.
  This model now correctly refs the fact table, not the silver model, so the
  already-cast submitted_at timestamptz column is available.
*/

with fact as (
    select * from {{ ref('fct_mtn_survey_responses') }}
),

daily_agg as (
    select
        submitted_at::date                                                          as submission_date,
        count(*)                                                                    as total_submissions,
        count(*) filter (where is_approved)                                         as approved_submissions,
        count(*) filter (where is_rejected)                                         as rejected_submissions,
        round(
            count(*) filter (where is_rejected)::numeric
            / nullif(count(*), 0) * 100
        , 2)                                                                        as rejection_rate_pct,
        round(avg(survey_duration_minutes)::numeric, 2)                             as avg_duration_minutes
    from fact
    where submitted_at is not null
    group by 1
)

select * from daily_agg
order by submission_date desc
