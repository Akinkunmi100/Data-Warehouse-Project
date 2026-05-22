{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer — QC flag summary dashboard table
  ---------------------------------------------
  Aggregated daily flag counts by form, schema, flag type, and severity.
  Intended for the Metabase "QC Overview" dashboard.

  Refreshed on every dbt build (after each nightly ETL run).
*/

with flags as (
    select
        f.client_schema,
        f.form_id,
        f.flag_type,
        f.severity,
        f.flagged_at::date                          as flag_date,
        count(*)                                    as flag_count,
        count(distinct f.submission_uuid)           as affected_submissions
    from {{ source('qc_system', 'qc_flags') }} f
    group by 1, 2, 3, 4, 5
),

enriched as (
    select
        flag_date,
        client_schema,
        form_id,
        flag_type,
        severity,
        flag_count,
        affected_submissions,
        -- Running 7-day total per form+flag_type for trend sparklines
        sum(flag_count) over (
            partition by client_schema, form_id, flag_type
            order by flag_date
            rows between 6 preceding and current row
        )                                           as rolling_7d_count
    from flags
)

select * from enriched
order by flag_date desc, client_schema, form_id, severity
