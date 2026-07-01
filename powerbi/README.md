# Power BI Dashboard Kit

This folder contains the Power BI-ready dashboard package for the research platform.

## Files

- `ResearchPlatform.Theme.json` - executive visual theme
- `measures.dax` - DAX measure pack for the semantic model
- `measures.tmdl` - bulk-create/update script for the `_calculations` measures table
- `report_blueprint.md` - page, visual, and relationship blueprint
- `model_catalog.md` - generated table/column catalog after validation

## Build Steps

1. Start PostgreSQL with `docker compose up -d`.
2. Build the reporting models:

   ```powershell
   cd dbt\research_platform
   dbt build
   cd ..\..
   ```

3. Validate the Power BI contract:

   ```powershell
   python scripts\validate_powerbi_model.py
   ```

4. Open Power BI Desktop.
5. Get Data > PostgreSQL database.
6. Server: `localhost:5435`
7. Database: `warehouse`
8. Select Import mode.
9. Load only these `gold` tables. Power BI displays them with the schema prefix, for example `gold fact_powerbi_state_quota`.

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

10. Import the theme from `ResearchPlatform.Theme.json`.
11. Create relationships using `report_blueprint.md`.
12. To create measures in bulk, open Power BI's TMDL view, paste `measures.tmdl`, select Preview, then Apply.

In DAX, table names with the `gold` prefix must be wrapped in single quotes, for example `'gold dim_powerbi_project'[project_name]`.

The report will work before quota data is loaded, but quota visuals become meaningful after importing real state targets with `scripts/import_state_quotas.py`.
