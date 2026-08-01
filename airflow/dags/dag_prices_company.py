"""
Daily prices + company info DAG.

Source: Yahoo Finance (yfinance). ingestion/yahoo_finance.py fetches BOTH
price history and company info in the same run (see fetch_stock_data /
save_price_history / save_company_info) - they share a source and a
schedule, so one DAG covers both rather than splitting them artificially.

Schedule: weekdays at 5pm ET, after US market close.

dbt work is scoped with --select stg_stock_prices+ stg_company_info+ so
this DAG only rebuilds the models that actually depend on this source
(int_company_metrics, mart_company, mart_health_score, company_scd
snapshot) - not the entire project. mart_health_score also depends on
stg_financials via int_financials_pivot, but that's fine: the mart always
reads the *latest available* row per ticker from each source (QUALIFY
ROW_NUMBER), so it naturally tolerates financials being up to a week
stale without needing a cross-DAG dependency.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from common import DEFAULT_ARGS, PROJECT_ROOT, alert_on_failure, dbt_cmd

SELECT = "stg_stock_prices+ stg_company_info+"

with DAG(
    dag_id="stockmentor_prices_company",
    description="Ingest daily prices + company info, transform, test, snapshot SCD.",
    default_args=DEFAULT_ARGS,
    schedule="0 17 * * 1-5",  # 5pm, Mon-Fri, in the timezone below
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    tags=["stockmentor", "prices", "company_info"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_yahoo_finance",
        bash_command=f"cd {PROJECT_ROOT} && python ingestion/yahoo_finance.py",
    )

    refresh_bronze_views = BashOperator(
        task_id="refresh_bronze_views",
        bash_command=f"cd {PROJECT_ROOT} && python scripts/setup_bronze_views.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=dbt_cmd(f"run --select {SELECT}"),
        on_failure_callback=alert_on_failure,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot_company_scd",
        bash_command=dbt_cmd("snapshot --select company_scd"),
        on_failure_callback=alert_on_failure,
    )

    # dbt exits non-zero only on severity:error test failures (unique/not_null
    # on keys). severity:warn tests (the accepted_range/accepted_values checks)
    # log a warning but return 0, so this task - and the DAG - succeeds through
    # "minor" data issues and only hard-stops on structural ones, per the
    # major/minor split we designed earlier.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_cmd(f"test --select {SELECT}"),
        on_failure_callback=alert_on_failure,
    )

    ingest >> refresh_bronze_views >> dbt_run >> dbt_snapshot >> dbt_test
