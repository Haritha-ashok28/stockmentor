"""
Weekly SEC EDGAR financials DAG.

Source: SEC EDGAR via edgartools (ingestion/sec_edgar.py) - income
statement, balance sheet, cash flow statement per ticker.

Schedule: weekly. Financial statements update on a quarterly filing
cadence, so daily runs would just re-check for no new data most of the
time - weekly is enough to catch a new quarterly filing promptly without
hammering SEC EDGAR.

dbt work is scoped with --select stg_financials+, covering
int_financials_pivot and mart_health_score - not stg_stock_prices or
stg_company_info, which this DAG has no reason to touch.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from common import DEFAULT_ARGS, PROJECT_ROOT, alert_on_failure, dbt_cmd

SELECT = "stg_financials+"

with DAG(
    dag_id="stockmentor_sec_edgar",
    description="Ingest SEC EDGAR financial statements, transform, test.",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * 1",  # Monday 6am ET
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    tags=["stockmentor", "financials", "sec_edgar"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_sec_edgar",
        bash_command=f"cd {PROJECT_ROOT} && python ingestion/sec_edgar.py",
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

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_cmd(f"test --select {SELECT}"),
        on_failure_callback=alert_on_failure,
    )

    ingest >> refresh_bronze_views >> dbt_run >> dbt_test
