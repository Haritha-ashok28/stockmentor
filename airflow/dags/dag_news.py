"""
News DAG - Yahoo Finance RSS, every 4 hours.

Source: ingestion/rss_news.py. Same-day dedup already happens inside the
ingestion script itself (save_to_parquet reads the existing day's parquet
file, concats, and drop_duplicates on `id`) - so this DAG doesn't need to
do anything extra for dedup, it's a property of the ingestion layer, not
the orchestration layer.

dbt work is scoped with --select stg_news+. There's no mart built on top
of news yet (per the gold-layer design, that's still on the roadmap), so
today this just builds/tests stg_news - the moment a news-dependent mart
exists, this same selector picks it up automatically without editing the
DAG.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from common import DEFAULT_ARGS, PROJECT_ROOT, alert_on_failure, dbt_cmd

SELECT = "stg_news+"

with DAG(
    dag_id="stockmentor_news",
    description="Ingest news every 4h, transform, test.",
    default_args=DEFAULT_ARGS,
    schedule="0 */4 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    tags=["stockmentor", "news"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_rss_news",
        bash_command=f"cd {PROJECT_ROOT} && python ingestion/rss_news.py",
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
