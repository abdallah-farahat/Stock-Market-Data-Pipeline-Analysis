"""
stock_market_pipeline_dag.py
────────────────────────────
Airflow DAG: Daily Stock Market ETL with Full Audit Logging

Schedule : daily at 06:00 UTC (after US market close)
Tickers  : AAPL, MSFT, AMZN, GOOGL, META, TSLA, NVDA, JPM, JNJ, V

Task Graph per ticker:
    extract_{ticker}  ──►  transform_{ticker}  ──►  load_{ticker}
                                                          │
All tickers                                               ▼
    ──────────────────────────────────────────►  audit_summary
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── import your pipeline modules ──────────────────────────────
# Adjust sys.path if your modules live outside the dags/ folder
import sys
sys.path.insert(0, "/opt/airflow/pipeline")

from etl   import extract_stock_data, transform_stock_data, load_to_database
from audit import RunLogger, get_conn

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  DAG-level config
# ─────────────────────────────────────────────────────────────
TICKERS = ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA", "JPM", "JNJ", "V"]

DEFAULT_ARGS = {
    "owner":            "data_team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ─────────────────────────────────────────────────────────────
#  Task callables
# ─────────────────────────────────────────────────────────────
def task_extract(ticker: str, **context) -> None:
    """Extract raw OHLCV data and push to XCom.

    Pushes an empty string to XCom when no new data is available so that
    downstream transform / load tasks can skip gracefully without failing.
    Missing data on a weekday is logged as a WARNING (visible in task logs)
    but does not raise an exception — the pipeline remains idempotent.
    """
    dag_run_id = context["run_id"]
    df = extract_stock_data(ticker, dag_run_id=dag_run_id)

    if df.empty:
        # Downstream tasks check for empty string and skip — not an error
        logger.warning(
            "[DAG] extract_%s — no new rows fetched. "
            "transform and load will be skipped for this ticker.", ticker
        )
        context["ti"].xcom_push(key=f"raw_{ticker}", value="")
        return

    # Serialize to JSON for XCom transport (suitable for daily incremental loads)
    context["ti"].xcom_push(
        key=f"raw_{ticker}",
        value=df.to_json(date_format="iso"),
    )
    logger.info("[DAG] extract_%s done — %d rows", ticker, len(df))


def task_transform(ticker: str, **context) -> None:
    """Pull raw data from XCom, run audit + transformations, push clean data."""
    import pandas as pd
    from io import StringIO

    dag_run_id = context["run_id"]
    # task_ids must be specified in Airflow 3.x — without it xcom_pull returns None
    raw_json   = context["ti"].xcom_pull(task_ids=f"extract_{ticker}", key=f"raw_{ticker}")

    if not raw_json:
        logger.info("[DAG] transform_%s — no data to transform", ticker)
        context["ti"].xcom_push(key=f"clean_{ticker}", value="")
        return

    raw_df   = pd.read_json(StringIO(raw_json))
    raw_df["date"] = pd.to_datetime(raw_df["date"]).dt.date

    clean_df = transform_stock_data(raw_df, ticker, dag_run_id=dag_run_id)
    context["ti"].xcom_push(
        key=f"clean_{ticker}",
        value=clean_df.to_json(date_format="iso") if not clean_df.empty else "",
    )
    logger.info("[DAG] transform_%s done — %d clean rows", ticker, len(clean_df))


def task_load(ticker: str, **context) -> None:
    """Pull clean data from XCom and load into PostgreSQL."""
    import pandas as pd
    from io import StringIO

    dag_run_id  = context["run_id"]
    # task_ids must be specified in Airflow 3.x — without it xcom_pull returns None
    clean_json  = context["ti"].xcom_pull(task_ids=f"transform_{ticker}", key=f"clean_{ticker}")

    if not clean_json:
        logger.info("[DAG] load_%s — nothing to load", ticker)
        return

    clean_df = pd.read_json(StringIO(clean_json))
    clean_df["date"] = pd.to_datetime(clean_df["date"]).dt.date

    result = load_to_database(clean_df, ticker, dag_run_id=dag_run_id)
    logger.info("[DAG] load_%s done — %s", ticker, result)


def task_audit_summary(**context) -> None:
    """
    Post-load audit: query the audit tables and print a run summary.
    Fails the DAG if any ERROR-level quality issues were recorded this run.
    """
    dag_run_id = context["run_id"]

    summary_sql = """
        SELECT
            task_name,
            SUM(records_found)    AS found,
            SUM(records_inserted) AS inserted,
            SUM(records_skipped)  AS skipped,
            SUM(records_invalid)  AS invalid,
            COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_tasks
        FROM audit_pipeline_log
        WHERE dag_run_id = %s
        GROUP BY task_name
        ORDER BY task_name
    """
    issues_sql = """
        SELECT COUNT(*) AS error_count
        FROM  audit_quality_issues qi
        JOIN  audit_pipeline_log   pl ON qi.run_id = pl.run_id
        WHERE pl.dag_run_id = %s
          AND qi.severity   = 'ERROR'
          AND qi.resolved   = FALSE
    """

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(summary_sql, (dag_run_id,))
            rows = cur.fetchall()
            logger.info("=" * 60)
            logger.info("AUDIT SUMMARY  —  DAG run: %s", dag_run_id)
            logger.info("%-20s %8s %10s %9s %9s %12s",
                        "task", "found", "inserted", "skipped", "invalid", "failed_tasks")
            for r in rows:
                logger.info("%-20s %8d %10d %9d %9d %12d", *r)
            logger.info("=" * 60)

            cur.execute(issues_sql, (dag_run_id,))
            error_count = cur.fetchone()[0]

    if error_count > 0:
        raise ValueError(
            f"[AUDIT] {error_count} unresolved ERROR-level quality issues detected. "
            "Check audit_quality_issues table."
        )
    logger.info("[AUDIT] All quality checks passed for run: %s", dag_run_id)


# ─────────────────────────────────────────────────────────────
#  DAG definition
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="stock_market_pipeline",
    description="Daily stock ETL: extract → transform (audit) → load → audit summary",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * 1-5",             # Mon–Fri at 06:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["stock", "etl", "audit"],
) as dag:

    load_tasks = []

    for ticker in TICKERS:

        extract_task = PythonOperator(
            task_id=f"extract_{ticker}",
            python_callable=task_extract,
            op_kwargs={"ticker": ticker},
        )

        transform_task = PythonOperator(
            task_id=f"transform_{ticker}",
            python_callable=task_transform,
            op_kwargs={"ticker": ticker},
        )

        load_task = PythonOperator(
            task_id=f"load_{ticker}",
            python_callable=task_load,
            op_kwargs={"ticker": ticker},
        )

        # ── per-ticker chain ──────────────────────────────────
        extract_task >> transform_task >> load_task
        load_tasks.append(load_task)

    # ── all loads must finish before audit summary ────────────
    audit_summary = PythonOperator(
        task_id="audit_summary",
        python_callable=task_audit_summary,
        trigger_rule="all_done",   # runs even if some tickers failed
    )

    load_tasks >> audit_summary
