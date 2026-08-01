"""
Shared config and helpers for all StockMentor DAGs.

Kept in one place (same DRY reasoning as the dbt incremental_filter macro)
so the three DAGs don't each hardcode paths, retry policy, and alerting
logic separately.
"""
import logging
from datetime import timedelta

from airflow.models import Variable

logger = logging.getLogger("airflow.stockmentor")

# Root of the StockMentor project on whatever machine/container runs Airflow.
# Set this once via `airflow variables set stockmentor_home /path/to/stockmentor`
# instead of hardcoding it in every DAG file.
PROJECT_ROOT = Variable.get("stockmentor_home", default_var="/opt/airflow/stockmentor")
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/dbt/stockmentor"
DBT_PROFILES_DIR = Variable.get("dbt_profiles_dir", default_var=DBT_PROJECT_DIR)

DEFAULT_ARGS = {
    "owner": "haritha",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": None,  # set per-task below via alert_on_failure
}


def alert_on_failure(context):
    """
    Placeholder alert hook. Wire this up to Slack/email/webhook later -
    for now it just logs loudly so failures aren't silent in a portfolio
    demo. Attach via `on_failure_callback=alert_on_failure` on any task
    where a hard failure (severity: error dbt test, or ingestion crash)
    should be visible immediately instead of only showing up in the UI.
    """
    ti = context["task_instance"]
    logger.error(
        "STOCKMENTOR ALERT: task %s in dag %s failed on %s",
        ti.task_id, ti.dag_id, context.get("execution_date"),
    )


def dbt_cmd(command: str) -> str:
    """Build a `cd <dbt dir> && dbt ...` bash command with profiles dir set."""
    return f"cd {DBT_PROJECT_DIR} && dbt {command} --profiles-dir {DBT_PROFILES_DIR}"
