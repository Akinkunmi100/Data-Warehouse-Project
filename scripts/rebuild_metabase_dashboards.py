"""Build a clean executive dashboard and one standalone dashboard per project."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2


ROOT = Path(__file__).resolve().parents[1]

PROJECTS = {
    "project_appraise": "Project Appraise",
    "cis_consumer_june": "CIS Consumer June",
    "construction_sites": "Construction Sites",
    "project_ojude_oba": "Project Ojude Oba",
    "retail_and_distributor_mortar_may_ending": "Retail And Distributor Mortar May Ending",
}


def load_env() -> None:
    for raw_line in (ROOT / "secrets" / ".env").read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def label_for_form(form_id: str) -> str:
    overrides = {
        "cis": "CIS",
        "mtn": "MTN",
    }
    words = []
    for part in form_id.replace("-", "_").split("_"):
        words.append(overrides.get(part.lower(), part.capitalize()))
    return " ".join(words)


def load_projects() -> dict[str, str]:
    registry_path = ROOT / "config" / "surveycto_forms.json"
    if not registry_path.exists():
        return PROJECTS
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    return {
        form_id: label_for_form(form_id)
        for form_id, config in sorted(registry.items())
        if config.get("active", True)
    }


def metabase_database_id(cur) -> int:
    preferred = os.getenv("METABASE_WAREHOUSE_DATABASE_ID")
    if preferred:
        return int(preferred)
    cur.execute(
        """
        select id
          from metabase_database
         where is_full_sync = true
           and lower(name) in ('warehouse', 'research platform', 'research data platform')
         order by id
         limit 1
        """
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("select id from metabase_database order by id limit 1")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("No Metabase database connection found. Connect Metabase to the warehouse first.")
    return row[0]


def native_query(sql: str, database_id: int) -> str:
    return json.dumps(
        {
            "lib/type": "mbql/query",
            "stages": [{"lib/type": "mbql.stage/native", "native": sql}],
            "database": database_id,
        }
    )


def upsert_dashboard(cur, name: str, description: str) -> int:
    cur.execute("select id from report_dashboard where name = %s order by id limit 1", (name,))
    row = cur.fetchone()
    if row:
        dashboard_id = row[0]
        cur.execute(
            """
            update report_dashboard
               set description = %s, updated_at = now(), archived = false, width = 'fixed'
             where id = %s
            """,
            (description, dashboard_id),
        )
        return dashboard_id

    cur.execute(
        """
        insert into report_dashboard (
            id, created_at, updated_at, name, description, creator_id, parameters,
            show_in_getting_started, enable_embedding, archived, auto_apply_filters,
            width, view_count, archived_directly, last_viewed_at
        )
        values (
            nextval('report_dashboard_id_seq'), now(), now(), %s, %s, 1, '[]',
            false, false, false, true, 'fixed', 0, false, now()
        )
        returning id
        """,
        (name, description),
    )
    return cur.fetchone()[0]


def upsert_card(cur, database_id: int, name: str, display: str, sql: str, description: str) -> int:
    cur.execute("select id from report_card where name = %s order by id limit 1", (name,))
    row = cur.fetchone()
    query = native_query(sql, database_id)
    if row:
        card_id = row[0]
        cur.execute(
            """
            update report_card
               set description = %s, display = %s, dataset_query = %s,
                   visualization_settings = '{}', updated_at = now(),
                   database_id = %s,
                   archived = false, cache_invalidated_at = now()
             where id = %s
            """,
            (description, display, query, database_id, card_id),
        )
        return card_id

    cur.execute(
        """
        insert into report_card (
            id, created_at, updated_at, name, description, display, dataset_query,
            visualization_settings, creator_id, database_id, query_type, archived,
            enable_embedding, collection_preview, type, cache_invalidated_at,
            last_used_at, view_count, archived_directly, card_schema
        )
        values (
            nextval('report_card_id_seq'), now(), now(), %s, %s, %s, %s, '{}',
            1, %s, 'native', false, false, true, 'question', now(), now(), 0, false, 20
        )
        returning id
        """,
        (name, description, display, query, database_id),
    )
    return cur.fetchone()[0]


def attach(cur, dashboard_id: int, card_id: int, row: int, col: int, size_x: int, size_y: int) -> None:
    cur.execute(
        """
        insert into report_dashboardcard (
            id, created_at, updated_at, size_x, size_y, row, col, card_id,
            dashboard_id, parameter_mappings, visualization_settings
        )
        values (
            nextval('report_dashboardcard_id_seq'), now(), now(), %s, %s, %s, %s,
            %s, %s, '[]', '{}'
        )
        """,
        (size_x, size_y, row, col, card_id, dashboard_id),
    )


def build_executive(cur, database_id: int) -> int:
    dashboard_id = upsert_dashboard(
        cur,
        "Research Platform Executive Overview",
        "Portfolio-level project volume, health, QC, and synchronization status.",
    )
    cur.execute("delete from report_dashboardcard where dashboard_id = %s", (dashboard_id,))

    cards = [
        ("Executive - Total Submissions", "scalar",
         "select coalesce(sum(total_submissions), 0) as total_submissions from gold.executive_overview",
         "Total submissions across all registered projects.", 0, 0, 6, 3),
        ("Executive - Active Projects", "scalar",
         "select count(*) as active_projects from gold.executive_overview where total_submissions > 0",
         "Projects containing at least one submission.", 0, 6, 6, 3),
        ("Executive - Projects Needing Attention", "scalar",
         "select count(*) as projects_needing_attention from gold.executive_overview where executive_status not in ('healthy', 'healthy_empty')",
         "Projects with an unhealthy executive status.", 0, 12, 6, 3),
        ("Executive - Total QC Flags", "scalar",
         "select coalesce(sum(total_qc_flags), 0) as total_qc_flags from gold.executive_overview",
         "Total active QC flags across all projects.", 0, 18, 6, 3),
        ("Executive - Total Quota", "scalar",
         "select coalesce(sum(quota_target), 0) as total_quota from gold.quota_vs_achievement_by_state where has_quota",
         "Total configured quota across all active projects and states.", 3, 0, 6, 3),
        ("Executive - Quota Achieved", "scalar",
         "select coalesce(sum(achieved_submissions), 0) as achieved_submissions from gold.quota_vs_achievement_by_state",
         "Total submissions counted toward configured quota views.", 3, 6, 6, 3),
        ("Executive - Achievement Rate", "scalar",
         """select round(
                  coalesce(sum(achieved_submissions), 0)::numeric
                  / nullif(sum(quota_target), 0) * 100,
                  2
                ) as achievement_rate_pct
              from gold.quota_vs_achievement_by_state
             where has_quota""",
         "Portfolio achievement percentage against configured quota.", 3, 12, 6, 3),
        ("Executive - States Behind Quota", "scalar",
         """select count(*) as states_behind_quota
              from gold.quota_vs_achievement_by_state
             where has_quota and achieved_submissions < quota_target""",
         "Number of project/state quota lines still below target.", 3, 18, 6, 3),
        ("Executive - Submissions by Project", "bar",
         "select form_id as project, total_submissions from gold.executive_overview order by total_submissions desc",
         "Submission volume by distinct project.", 6, 0, 12, 6),
        ("Executive - Quota vs Achievement by Project", "bar",
         """select form_id as project,
                   coalesce(sum(quota_target), 0) as quota_target,
                   sum(achieved_submissions) as achieved_submissions
              from gold.quota_vs_achievement_by_state
             group by form_id
             order by quota_target desc, achieved_submissions desc""",
         "Project-level quota target compared with achieved submissions.", 6, 12, 12, 6),
        ("Executive - Project Status Table", "table",
         """select form_id as project, total_submissions, executive_status, last_run_status,
                   last_successful_sync, minutes_since_last_success, is_sla_breached,
                   total_qc_flags, critical_qc_flags, high_qc_flags,
                   flagged_submission_rate_pct, schema_version_count
              from gold.executive_overview order by total_submissions desc""",
         "Complete operational status for each project.", 12, 0, 12, 8),
        ("Executive - State Quota Detail", "table",
         """select form_id as project, state_name, wave_name, quota_target,
                   achieved_submissions, achievement_rate_pct, remaining_to_quota, quota_status
              from gold.quota_vs_achievement_by_state
             order by has_quota desc, remaining_to_quota desc nulls last, achieved_submissions desc""",
         "State-level quota progress for every project.", 12, 12, 12, 8),
        ("Executive - Pipeline Health", "table",
         "select * from gold.pipeline_health order by is_sla_breached desc, pipeline_name",
         "Pipeline SLA and synchronization health.", 20, 0, 24, 7),
        ("Executive - QC Flags Last 7 Days", "bar",
         """select form_id, severity, sum(flag_count) as flags
              from gold.qc_flag_summary
             where flag_date >= current_date - interval '7 days'
             group by form_id, severity
             order by flags desc""",
         "Recent QC flag volume by project and severity.", 27, 0, 12, 7),
        ("Executive - Daily Portfolio Trend", "line",
         """select submission_date, sum(total_submissions) as total_submissions
              from gold.project_daily_submissions
             group by submission_date
             order by submission_date""",
         "Daily submission trend across all projects.", 27, 12, 12, 7),
    ]
    for name, display, sql, description, row, col, sx, sy in cards:
        attach(cur, dashboard_id, upsert_card(cur, database_id, name, display, sql, description), row, col, sx, sy)
    return dashboard_id


def build_qc_overview(cur, database_id: int) -> int:
    dashboard_id = upsert_dashboard(
        cur,
        "Research Platform QC Overview",
        "Portfolio-level quality-control flag trends, severity mix, and affected submissions.",
    )
    cur.execute("delete from report_dashboardcard where dashboard_id = %s", (dashboard_id,))

    cards = [
        ("QC - Total Flags", "scalar",
         "select coalesce(sum(flag_count), 0) as total_flags from gold.qc_flag_summary",
         "Total QC flags across all projects.", 0, 0, 6, 3),
        ("QC - Affected Submissions", "scalar",
         "select coalesce(sum(affected_submissions), 0) as affected_submissions from gold.qc_flag_summary",
         "Total affected submission counts across flag groups.", 0, 6, 6, 3),
        ("QC - Critical Flags", "scalar",
         "select coalesce(sum(flag_count), 0) as critical_flags from gold.qc_flag_summary where severity = 'critical'",
         "Critical QC flags across all projects.", 0, 12, 6, 3),
        ("QC - High Flags", "scalar",
         "select coalesce(sum(flag_count), 0) as high_flags from gold.qc_flag_summary where severity = 'high'",
         "High severity QC flags across all projects.", 0, 18, 6, 3),
        ("QC - Flags by Project", "bar",
         """select form_id, severity, sum(flag_count) as flags
              from gold.qc_flag_summary
             group by form_id, severity
             order by flags desc""",
         "QC flags by project and severity.", 3, 0, 12, 7),
        ("QC - Daily Trend", "line",
         """select flag_date, severity, sum(flag_count) as flags
              from gold.qc_flag_summary
             group by flag_date, severity
             order by flag_date""",
         "Daily QC flag trend by severity.", 3, 12, 12, 7),
        ("QC - Flag Type Detail", "table",
         """select form_id, flag_type, severity, sum(flag_count) as flags,
                   sum(affected_submissions) as affected_submissions,
                   max(rolling_7d_count) as latest_rolling_7d_count
              from gold.qc_flag_summary
             group by form_id, flag_type, severity
             order by flags desc, form_id, flag_type""",
         "Flag type detail for triage.", 10, 0, 24, 8),
    ]
    for name, display, sql, description, row, col, sx, sy in cards:
        attach(cur, dashboard_id, upsert_card(cur, database_id, name, display, sql, description), row, col, sx, sy)
    return dashboard_id


def build_field_team(cur, database_id: int) -> int:
    dashboard_id = upsert_dashboard(
        cur,
        "Research Platform Field Team Leaderboard",
        "Enumerator scorecards, flag rates, and project-level fieldwork performance.",
    )
    cur.execute("delete from report_dashboardcard where dashboard_id = %s", (dashboard_id,))

    cards = [
        ("Field Team - Enumerators", "scalar",
         "select count(distinct enumerator_id) as enumerators from gold.enumerator_scorecard",
         "Total enumerators with computed scorecards.", 0, 0, 6, 3),
        ("Field Team - Average Score", "scalar",
         "select round(avg(quality_score), 2) as avg_quality_score from gold.enumerator_scorecard",
         "Average enumerator quality score.", 0, 6, 6, 3),
        ("Field Team - Total Submissions", "scalar",
         "select coalesce(sum(total_submissions), 0) as total_submissions from gold.enumerator_scorecard",
         "Submissions represented in scorecards.", 0, 12, 6, 3),
        ("Field Team - Total Flags", "scalar",
         "select coalesce(sum(total_flags), 0) as total_flags from gold.enumerator_scorecard",
         "QC flags represented in scorecards.", 0, 18, 6, 3),
        ("Field Team - Best Scores", "table",
         """select form_id, enumerator_id, total_submissions, total_flags,
                   flag_rate, quality_score, rank_in_form
              from gold.enumerator_scorecard
             order by quality_score desc nulls last, total_submissions desc
             limit 25""",
         "Top scoring enumerators across projects.", 3, 0, 12, 8),
        ("Field Team - Needs Review", "table",
         """select form_id, enumerator_id, total_submissions, total_flags,
                   critical_flags, high_flags, flag_rate, quality_score, rank_in_form
              from gold.enumerator_scorecard
             where total_flags > 0
             order by critical_flags desc, high_flags desc, flag_rate desc nulls last
             limit 50""",
         "Enumerators with the highest quality risk.", 3, 12, 12, 8),
        ("Field Team - Score Distribution", "bar",
         """select form_id,
                   width_bucket(coalesce(quality_score, 0), 0, 100, 5) as score_bucket,
                   count(*) as enumerators
              from gold.enumerator_scorecard
             group by form_id, score_bucket
             order by form_id, score_bucket""",
         "Quality score distribution by project.", 11, 0, 12, 7),
        ("Field Team - Project Summary", "table",
         """select form_id,
                   count(distinct enumerator_id) as enumerators,
                   sum(total_submissions) as total_submissions,
                   sum(total_flags) as total_flags,
                   round(avg(quality_score), 2) as avg_quality_score,
                   round(avg(flag_rate), 4) as avg_flag_rate
              from gold.enumerator_scorecard
             group by form_id
             order by avg_quality_score desc nulls last""",
         "Project-level field team performance.", 11, 12, 12, 7),
    ]
    for name, display, sql, description, row, col, sx, sy in cards:
        attach(cur, dashboard_id, upsert_card(cur, database_id, name, display, sql, description), row, col, sx, sy)
    return dashboard_id


def build_project(cur, database_id: int, form_id: str, label: str) -> int:
    dashboard_id = upsert_dashboard(
        cur,
        f"Project - {label}",
        f"Standalone submissions, fieldwork achievement, duplicates, QC, and pipeline health for {label}.",
    )
    cur.execute("delete from report_dashboardcard where dashboard_id = %s", (dashboard_id,))
    form = form_id.replace("'", "''")
    prefix = f"{label} -"

    cards = [
        (f"{prefix} Total Submissions", "scalar",
         f"select total_submissions from gold.executive_overview where form_id = '{form}'",
         f"Total submissions for {label}.", 0, 0, 6, 3),
        (f"{prefix} Executive Status", "scalar",
         f"select executive_status from gold.executive_overview where form_id = '{form}'",
         f"Current executive status for {label}.", 0, 6, 6, 3),
        (f"{prefix} QC Flags", "scalar",
         f"select total_qc_flags from gold.executive_overview where form_id = '{form}'",
         f"Total QC flags for {label}.", 0, 12, 6, 3),
        (f"{prefix} Duplicate Respondents", "scalar",
         f"select coalesce(sum(duplicate_count), 0) as duplicate_respondents from gold.duplicates_by_state_respondent where form_id = '{form}'",
         f"Repeated respondent identifiers for {label}.", 0, 18, 6, 3),
        (f"{prefix} Quota Target", "scalar",
         f"select coalesce(sum(quota_target), 0) as quota_target from gold.quota_vs_achievement_by_state where form_id = '{form}' and has_quota",
         f"Configured quota target for {label}.", 3, 0, 6, 3),
        (f"{prefix} Quota Achievement Rate", "scalar",
         f"""select round(
                   coalesce(sum(achieved_submissions), 0)::numeric
                   / nullif(sum(quota_target), 0) * 100,
                   2
                 ) as achievement_rate_pct
               from gold.quota_vs_achievement_by_state
              where form_id = '{form}' and has_quota""",
         f"Achievement rate against configured quota for {label}.", 3, 6, 6, 3),
        (f"{prefix} Remaining Quota", "scalar",
         f"select coalesce(sum(remaining_to_quota), 0) as remaining_to_quota from gold.quota_vs_achievement_by_state where form_id = '{form}' and has_quota",
         f"Remaining quota required for {label}.", 3, 12, 6, 3),
        (f"{prefix} States Behind Quota", "scalar",
         f"""select count(*) as states_behind_quota
               from gold.quota_vs_achievement_by_state
              where form_id = '{form}' and has_quota and achieved_submissions < quota_target""",
         f"States still below quota for {label}.", 3, 18, 6, 3),
        (f"{prefix} Quota vs Achievement by State", "bar",
         f"""select state_name, quota_target, achieved_submissions
               from gold.quota_vs_achievement_by_state
              where form_id = '{form}' and has_quota
              union all
             select 'No quota loaded', 0, 0
              where not exists (
                    select 1 from gold.quota_vs_achievement_by_state
                     where form_id = '{form}' and has_quota
              )
              order by quota_target desc, achieved_submissions desc""",
         f"Configured quota compared with achieved submissions by state for {label}.", 6, 0, 12, 7),
        (f"{prefix} State Quota Detail", "table",
         f"""select state_name, wave_name, quota_target, achieved_submissions,
                    approved_submissions, achievement_rate_pct, remaining_to_quota, quota_status
               from gold.quota_vs_achievement_by_state
              where form_id = '{form}'
              union all
             select 'No quota or submissions yet', 'default', null::integer, 0, 0, null::numeric, null::integer, 'no_data'
              where not exists (
                    select 1 from gold.quota_vs_achievement_by_state
                     where form_id = '{form}'
              )
              order by remaining_to_quota desc nulls last, achieved_submissions desc""",
         f"Detailed state quota progress for {label}.", 6, 12, 12, 7),
        (f"{prefix} Achievement by State", "bar",
         f"""select state_name, sum(total_submissions) as submissions,
                    sum(approved_submissions) as approved
               from gold.achievements_by_state_respondent
              where form_id = '{form}'
              group by state_name
              union all
             select 'No submissions yet', 0, 0
              where not exists (
                    select 1 from gold.achievements_by_state_respondent
                     where form_id = '{form}'
              )
              order by submissions desc""",
         f"Submission achievement by state for {label}.", 13, 0, 12, 7),
        (f"{prefix} Interviewer Achievement", "table",
         f"""select interviewer_id, sum(total_submissions) as total_submissions,
                    sum(approved_submissions) as approved_submissions,
                    sum(rejected_submissions) as rejected_submissions
               from gold.achievements_by_state_respondent
              where form_id = '{form}'
              group by interviewer_id
              union all
             select 'No submissions yet', 0, 0, 0
              where not exists (
                    select 1 from gold.achievements_by_state_respondent
                     where form_id = '{form}'
              )
              order by total_submissions desc""",
         f"Achievement by interviewer for {label}.", 13, 12, 12, 7),
        (f"{prefix} Daily Submissions", "bar",
         f"""select submission_date, total_submissions, approved_submissions, rejected_submissions
               from gold.project_daily_submissions
              where form_id = '{form}'
              union all
             select current_date, 0, 0, 0
              where not exists (
                    select 1 from gold.project_daily_submissions
                     where form_id = '{form}'
              )
              order by submission_date""",
         f"Daily submission trend for {label}.", 20, 0, 12, 7),
        (f"{prefix} State Performance", "table",
         f"""select region_name as state_name, total_submissions, approved_submissions,
                    rejected_submissions, approval_rate_pct, rank_by_volume
               from gold.regional_performance
              where form_id = '{form}'
              union all
             select 'No submissions yet', 0, 0, 0, null::numeric, 0
              where not exists (
                    select 1 from gold.regional_performance
                     where form_id = '{form}'
              )
              order by rank_by_volume""",
         f"State performance metrics for {label}.", 20, 12, 12, 7),
        (f"{prefix} Duplicate Respondents Detail", "table",
         f"""select state_name, respondent_id, submission_count, duplicate_count
               from gold.duplicates_by_state_respondent
              where form_id = '{form}'
              union all
             select 'No duplicates found', null::text, 0, 0
              where not exists (
                    select 1 from gold.duplicates_by_state_respondent
                     where form_id = '{form}'
              )
              order by duplicate_count desc, state_name""",
         f"Repeated respondent identifiers for {label}.", 27, 0, 12, 7),
        (f"{prefix} Enumerator Scorecard", "table",
         f"""select enumerator_id, total_submissions, total_flags, critical_flags,
                    high_flags, medium_flags, flag_rate, quality_score, rank_in_form
               from gold.enumerator_scorecard
              where form_id = '{form}'
              union all
             select 'No submissions yet', 0, 0, 0, 0, 0, null::numeric, null::numeric, 0
              where not exists (
                    select 1 from gold.enumerator_scorecard
                     where form_id = '{form}'
              )
              order by rank_in_form, total_submissions desc""",
         f"QC scorecard for field devices/enumerators in {label}.", 27, 12, 12, 7),
        (f"{prefix} Pipeline and QC Health", "table",
         f"""select form_id as project, last_run_status, last_successful_sync,
                    minutes_since_last_success, is_sla_breached, total_qc_flags,
                    critical_qc_flags, high_qc_flags, flagged_submission_rate_pct,
                    schema_version_count, executive_status
               from gold.executive_overview where form_id = '{form}'""",
         f"Pipeline and QC health for {label}.", 34, 0, 24, 5),
        (f"{prefix} QC Flag Summary", "table",
         f"""select flag_date, flag_type, severity, flag_count, affected_submissions,
                    rolling_7d_count
               from gold.qc_flag_summary
              where form_id = '{form}'
              union all
             select current_date, 'No QC flags', 'low', 0, 0, 0
              where not exists (
                    select 1 from gold.qc_flag_summary
                     where form_id = '{form}'
              )
              order by flag_date desc, severity, flag_type""",
         f"QC flag trend and severity detail for {label}.", 39, 0, 12, 7),
        (f"{prefix} Submission Status Mix", "bar",
         f"""select 'approved' as status, coalesce(sum(approved_submissions), 0) as submissions
               from gold.project_daily_submissions where form_id = '{form}'
              union all
             select 'rejected', coalesce(sum(rejected_submissions), 0)
               from gold.project_daily_submissions where form_id = '{form}'
              union all
             select 'pending/unknown', coalesce(sum(pending_or_unknown_submissions), 0)
               from gold.project_daily_submissions where form_id = '{form}'""",
         f"Review status mix for {label}.", 39, 12, 12, 7),
    ]
    for name, display, sql, description, row, col, sx, sy in cards:
        attach(cur, dashboard_id, upsert_card(cur, database_id, name, display, sql, description), row, col, sx, sy)
    return dashboard_id


def main() -> None:
    load_env()
    projects = load_projects()
    connection = psycopg2.connect(
        host="localhost",
        port=5435,
        dbname=os.environ["METABASE_DB_NAME"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        with connection:
            with connection.cursor() as cur:
                database_id = metabase_database_id(cur)
                dashboards = {
                    "Executive": build_executive(cur, database_id),
                    "QC Overview": build_qc_overview(cur, database_id),
                    "Field Team": build_field_team(cur, database_id),
                }
                for form_id, label in projects.items():
                    dashboards[label] = build_project(cur, database_id, form_id, label)
        for label, dashboard_id in dashboards.items():
            print(f"{label}: http://localhost:3030/dashboard/{dashboard_id}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
