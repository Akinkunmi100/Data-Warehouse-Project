# Power BI Model Catalog

Generated from live PostgreSQL metadata.

## gold.dim_powerbi_project

Rows at validation time: `3`

| Column | Type | Nullable |
|---|---|---|
| `project_name` | text | YES |
| `total_submissions` | bigint | YES |
| `data_status` | text | YES |
| `sync_status` | text | YES |
| `dashboard_status` | text | YES |
| `last_run_status` | text | YES |
| `last_successful_sync` | timestamp with time zone | YES |
| `minutes_since_last_success` | numeric | YES |
| `is_sla_breached` | boolean | YES |
| `total_qc_flags` | bigint | YES |
| `critical_qc_flags` | bigint | YES |
| `high_qc_flags` | bigint | YES |
| `flagged_submission_rate_pct` | numeric | YES |
| `schema_version_count` | bigint | YES |

## gold.dim_powerbi_state

Rows at validation time: `31`

| Column | Type | Nullable |
|---|---|---|
| `state_key` | text | YES |
| `state_name` | text | YES |
| `is_real_state` | boolean | YES |

## gold.fact_powerbi_state_quota

Rows at validation time: `43`

| Column | Type | Nullable |
|---|---|---|
| `project_name` | text | YES |
| `state_key` | text | YES |
| `state_name` | text | YES |
| `wave_name` | text | YES |
| `quota_target` | integer | YES |
| `achieved_submissions` | integer | YES |
| `approved_submissions` | integer | YES |
| `rejected_submissions` | integer | YES |
| `pending_or_unknown_submissions` | integer | YES |
| `achievement_rate_pct` | numeric | YES |
| `remaining_to_quota` | integer | YES |
| `quota_status` | text | YES |
| `has_quota` | boolean | YES |
| `latest_submission_date` | date | YES |
| `quota_notes` | text | YES |

## gold.fact_powerbi_project_daily

Rows at validation time: `26`

| Column | Type | Nullable |
|---|---|---|
| `project_name` | text | YES |
| `submission_date` | date | YES |
| `total_submissions` | bigint | YES |
| `approved_submissions` | bigint | YES |
| `rejected_submissions` | bigint | YES |
| `pending_or_unknown_submissions` | bigint | YES |

## gold.fact_powerbi_qc_summary

Rows at validation time: `0`

| Column | Type | Nullable |
|---|---|---|
| `project_name` | text | YES |
| `flag_date` | date | YES |
| `flag_type` | text | YES |
| `severity` | text | YES |
| `flag_count` | bigint | YES |
| `affected_submissions` | bigint | YES |
| `rolling_7d_count` | numeric | YES |

## gold.fact_powerbi_enumerator_scorecard

Rows at validation time: `392`

| Column | Type | Nullable |
|---|---|---|
| `project_name` | text | YES |
| `enumerator_name` | text | YES |
| `total_submissions` | integer | YES |
| `total_flags` | integer | YES |
| `critical_flags` | integer | YES |
| `high_flags` | integer | YES |
| `medium_flags` | integer | YES |
| `quality_score` | numeric | YES |
| `rank_in_form` | bigint | YES |
| `flag_rate` | numeric | YES |
| `quality_band` | text | YES |

## gold.fact_powerbi_duplicates

Rows at validation time: `11`

| Column | Type | Nullable |
|---|---|---|
| `project_name` | text | YES |
| `state_key` | text | YES |
| `state_name` | text | YES |
| `enumerator_name` | text | YES |
| `enumerator_count` | bigint | YES |
| `respondent_reference` | text | YES |
| `submission_count` | bigint | YES |
| `duplicate_count` | bigint | YES |
| `duplicate_severity` | text | YES |

## gold.fact_powerbi_regional_performance

Rows at validation time: `43`

| Column | Type | Nullable |
|---|---|---|
| `project_name` | text | YES |
| `state_key` | text | YES |
| `state_name` | text | YES |
| `total_submissions` | numeric | YES |
| `approved_submissions` | numeric | YES |
| `rejected_submissions` | numeric | YES |
| `approval_rate_pct` | numeric | YES |
| `rank_by_volume` | bigint | YES |
| `approval_band` | text | YES |

## gold.fact_powerbi_pipeline_health

Rows at validation time: `3`

| Column | Type | Nullable |
|---|---|---|
| `project_name` | text | YES |
| `last_run_status` | text | YES |
| `last_successful_sync` | timestamp with time zone | YES |
| `last_attempted_at` | timestamp with time zone | YES |
| `minutes_since_last_success` | numeric | YES |
| `max_lag_minutes` | integer | YES |
| `is_sla_breached` | boolean | YES |
| `pipeline_status` | text | YES |

## gold.powerbi_project_risk_summary

Rows at validation time: `3`

| Column | Type | Nullable |
|---|---|---|
| `project_name` | text | YES |
| `total_submissions` | bigint | YES |
| `data_status` | text | YES |
| `sync_status` | text | YES |
| `dashboard_status` | text | YES |
| `total_qc_flags` | bigint | YES |
| `critical_qc_flags` | bigint | YES |
| `high_qc_flags` | bigint | YES |
| `flagged_submission_rate_pct` | numeric | YES |
| `quota_target` | bigint | YES |
| `achieved_submissions` | bigint | YES |
| `remaining_to_quota` | bigint | YES |
| `states_behind_quota` | bigint | YES |
| `duplicate_count` | numeric | YES |
| `duplicated_respondents` | bigint | YES |
| `enumerator_count` | bigint | YES |
| `avg_quality_score` | numeric | YES |
| `enumerators_needing_review` | bigint | YES |
| `states_with_submissions` | bigint | YES |
| `avg_state_approval_rate` | numeric | YES |
| `states_needing_review` | bigint | YES |
| `pipeline_status` | text | YES |
| `is_sla_breached` | boolean | YES |
| `operational_risk_score` | integer | YES |
| `operational_risk_band` | text | YES |

## gold.powerbi_kpi_snapshot

Rows at validation time: `1`

| Column | Type | Nullable |
|---|---|---|
| `snapshot_at` | timestamp with time zone | YES |
| `total_quota` | integer | YES |
| `achieved_submissions` | integer | YES |
| `approved_submissions` | integer | YES |
| `rejected_submissions` | integer | YES |
| `pending_or_unknown_submissions` | integer | YES |
| `achievement_rate_pct` | numeric | YES |
| `remaining_to_quota` | integer | YES |
| `states_behind_quota` | integer | YES |
| `active_projects` | integer | YES |
| `loaded_projects` | integer | YES |
| `total_qc_flags` | integer | YES |
| `critical_qc_flags` | integer | YES |
| `high_qc_flags` | integer | YES |
| `duplicate_count` | integer | YES |
| `enumerators_needing_review` | integer | YES |
| `states_needing_review` | integer | YES |
| `max_project_risk_score` | integer | YES |
| `high_risk_projects` | integer | YES |

## Data Readiness Note

`qc_system.project_state_quotas` has 0 rows. The report structure is ready, but quota visuals need real state targets.
