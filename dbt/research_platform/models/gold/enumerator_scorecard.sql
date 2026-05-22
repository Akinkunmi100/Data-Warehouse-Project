{{
  config(
    materialized = 'table',
    schema       = 'gold'
  )
}}

/*
  Gold layer — Enumerator scorecard leaderboard
  -----------------------------------------------
  Exposes the latest computed quality scores for every enumerator across all
  forms. Intended for the Metabase "Field Team Leaderboard" dashboard.

  Source: qc_system.enumerator_scores (populated by qc_engine.compute_scores).
  Ranked within each form so Metabase can filter to a single campaign.
*/

with scores as (
    select
        enumerator_id,
        form_id,
        client_schema,
        total_submissions,
        total_flags,
        critical_flags,
        high_flags,
        medium_flags,
        quality_score,
        computed_at,

        -- Rank within form: 1 = best enumerator
        rank() over (
            partition by form_id, client_schema
            order by quality_score desc nulls last
        )                                           as rank_in_form,

        -- Flag rate: flags per submission (lower is better)
        case
            when total_submissions > 0
            then round(total_flags::numeric / total_submissions, 4)
            else null
        end                                         as flag_rate

    from {{ source('qc_system', 'enumerator_scores') }}
)

select * from scores
order by client_schema, form_id, rank_in_form
