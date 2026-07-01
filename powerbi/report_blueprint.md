# Research Platform Power BI Dashboard Blueprint

This blueprint is the build standard for the executive Power BI report. It is designed for a polished operational dashboard similar in density and polish to Onyx/ZoomCharts-style executive dashboards, while remaining fully backed by the PostgreSQL warehouse.

## Data Connection

- Connector: PostgreSQL database
- Server: `localhost:5435`
- Database: `warehouse`
- Mode: Import for best visual performance; DirectQuery only if the report must be live minute-by-minute
- Schema: `gold`

Load these tables:

- `gold dim_powerbi_project`
- `gold dim_powerbi_state`
- `gold fact_powerbi_state_quota`
- `gold fact_powerbi_project_daily`
- `gold fact_powerbi_qc_summary`
- `gold fact_powerbi_enumerator_scorecard`
- `gold fact_powerbi_duplicates`
- `gold fact_powerbi_regional_performance`
- `gold fact_powerbi_pipeline_health`
- `gold powerbi_project_risk_summary`
- `gold powerbi_kpi_snapshot`

## Relationships

- `gold dim_powerbi_project[project_name]` one-to-many `gold fact_powerbi_state_quota[project_name]`
- `gold dim_powerbi_project[project_name]` one-to-many `gold fact_powerbi_project_daily[project_name]`
- `gold dim_powerbi_project[project_name]` one-to-many `gold fact_powerbi_qc_summary[project_name]`
- `gold dim_powerbi_project[project_name]` one-to-many `gold fact_powerbi_enumerator_scorecard[project_name]`
- `gold dim_powerbi_project[project_name]` one-to-many `gold fact_powerbi_duplicates[project_name]`
- `gold dim_powerbi_project[project_name]` one-to-many `gold fact_powerbi_regional_performance[project_name]`
- `gold dim_powerbi_project[project_name]` one-to-many `gold fact_powerbi_pipeline_health[project_name]`
- `gold dim_powerbi_project[project_name]` one-to-one `gold powerbi_project_risk_summary[project_name]`
- `gold dim_powerbi_state[state_key]` one-to-many `gold fact_powerbi_state_quota[state_key]`
- `gold dim_powerbi_state[state_key]` one-to-many `gold fact_powerbi_duplicates[state_key]`
- `gold dim_powerbi_state[state_key]` one-to-many `gold fact_powerbi_regional_performance[state_key]`

Cross-filter direction should remain single direction from dimensions to facts.

For DAX formulas, quote these table names because they contain spaces, for example `'gold fact_powerbi_state_quota'[quota_target]`.

## Theme

Import `powerbi/ResearchPlatform.Theme.json`.

The design uses:

- White canvas with restrained dark text
- Blue for primary volume
- Green for achieved/met
- Amber for risk/on-track warning
- Red for behind/critical
- Compact cards and tables for executive scanning

## Page 1: Executive Overview

Canvas: 16:9.

Top filter rail:

- Project slicer from `gold dim_powerbi_project[project_name]`
- Wave slicer from `gold fact_powerbi_state_quota[wave_name]`
- Status slicer from `gold fact_powerbi_state_quota[quota_status]`

KPI row:

- Total Quota
- Achieved Submissions
- Achievement Rate %
- Remaining To Quota
- Max Project Risk Score
- Projects Needing Attention
- Total QC Flags

Main visuals:

- Clustered bar: quota target vs achieved submissions by project
- Line chart: daily submissions by date
- Matrix: project, dashboard status, operational risk, quota, achieved, achievement rate, remaining, QC flags, duplicates, sync status
- Donut: operational risk band
- Small table: top 10 projects requiring attention, sorted by operational risk score

Executive standard:

- Cards should use no more than 6 KPIs above the fold
- Use conditional color on achievement rate: red below 50%, amber 50-79%, green 80%+
- Put operational warnings near the top right
- Use risk band as the executive narrative: healthy, watch, high, critical

## Page 2: State Quota Command Center

Purpose: compare quota to achievement from each state.

Visuals:

- Filled map or shape map by state using achievement rate
- Bar chart by state: quota target vs achieved submissions
- Table: state, project, quota, achieved, achievement rate, remaining, status, latest submission date
- Top gap visual: states with largest remaining quota

Conditional formatting:

- `behind`: red
- `not_started`: red
- `on_track`: amber
- `met`: green
- `no_quota`: gray

## Page 3: Project Drilldown

Purpose: focus on one project.

Required filter:

- Single-select project slicer

Visuals:

- KPI cards: project quota, achieved, achievement rate, remaining, QC flags
- Daily submission trend
- State quota detail table
- Enumerator leaderboard from `gold fact_powerbi_enumerator_scorecard`
- Duplicate respondent table from `gold fact_powerbi_duplicates`
- Project risk breakdown from `gold powerbi_project_risk_summary`

## Page 4: Quality and Operations

Visuals:

- Total QC flags
- Critical QC flags
- High QC flags
- QC flags by severity
- QC trend by date
- Pipeline status table from `gold dim_powerbi_project`
- Duplicate severity table
- Top enumerators needing review
- State approval-rate table

## Page 5: Field Team Performance

Purpose: manage field productivity and quality.

Visuals:

- KPI cards: Enumerator Count, Average Quality Score, Enumerators Needing Review, Average Enumerator Flag Rate
- Leaderboard: enumerator, submissions, flags, quality score, rank
- Bar chart: submissions by enumerator
- Scatter: total submissions vs quality score, colored by quality band
- Table: enumerators needing review with critical/high/medium flags

## Page 6: Data Quality and Duplicate Risk

Purpose: find suspicious records and repeated respondent identifiers quickly.

Visuals:

- KPI cards: Duplicate Count, Duplicated Respondents, Duplicate Severity Critical
- Table: project, state, enumerator name, respondent reference, submission count, duplicate count, severity
- Bar chart: duplicate count by enumerator name
- Bar chart: duplicate count by project
- Bar chart: duplicate count by state

## Page 7: Pipeline Health

Purpose: monitor automation reliability.

Visuals:

- KPI cards: Pipeline Breaches, Failed Pipelines, Average Minutes Since Sync
- Table: project, pipeline status, last run status, last successful sync, minutes since last success, SLA breach
- Conditional formatting: failed red, stale/attention amber, healthy green

Use these status fields:

- `data_status`: whether the project has loaded data (`loaded` or `empty`)
- `sync_status`: whether the latest automation is healthy, stale, failed, or not synced
- `dashboard_status`: business-friendly combined label for executive visuals

## Optional ZoomCharts Visuals

If ZoomCharts custom visuals are installed:

- Use Drill Down Combo PRO for quota vs achievement by project/state
- Use Drill Down Donut PRO for quota status mix
- Use Drill Down Timeline PRO for daily submissions
- Use Drill Down Graph PRO for project-to-state performance exploration
- Use Drill Down Combo PRO for enumerator productivity vs quality
- Keep the standard Power BI visuals as fallback so the report remains portable

## Publish Checklist

- Import theme
- Load only `gold dim_powerbi_*`, `gold fact_powerbi_*`, `gold powerbi_project_risk_summary`, and `gold powerbi_kpi_snapshot`
- Add DAX measures from `powerbi/measures.dax`
- Use `project_name`, `state_name`, and `enumerator_name` in visuals instead of technical IDs
- Format percentage measures as percentages with one decimal
- Validate with `python scripts/validate_powerbi_model.py`
