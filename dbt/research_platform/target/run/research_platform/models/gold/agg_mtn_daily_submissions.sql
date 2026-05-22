
  
    

  create  table "warehouse"."public_gold"."agg_mtn_daily_submissions__dbt_tmp"
  
  
    as
  
  (
    -- models/gold/agg_mtn_daily_submissions.sql


with fact as (
    select * from "warehouse"."public_gold"."fct_mtn_survey_responses"
),
daily_agg as (
    select
        cast(submitted_at as date) as submission_date,
        count(*) as total_submissions,
        sum(case when review_status = 'approved' then 1 else 0 end) as approved_submissions,
        sum(case when review_status = 'rejected' then 1 else 0 end) as rejected_submissions,
        round(sum(case when review_status = 'rejected' then 1.0 else 0.0 end) / nullif(count(*), 0) * 100, 2) as rejection_rate_pct,
        avg(survey_duration_minutes) as avg_duration_minutes
    from fact
    where submitted_at is not null
    group by 1
)

select * from daily_agg
order by submission_date desc
  );
  