"""
### Daily order KPIs

Runs after midnight UTC and rolls the previous day's raw events (written continuously by the
Spark streaming job) into `daily_order_kpis`.

1. **check_source_freshness** - fail fast if no events landed for the day.
2. **check_no_duplicate_events** - surfaces how much the at-least-once stream re-delivered.
3. **build_daily_kpis** - idempotent `MERGE`, de-duplicating by `event_id`; safe to re-run or backfill.
4. **check_kpis_written** / **check_success_rate_bounds** - guard the output before anyone reads it.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)

PROJECT = os.environ.get("RTA_GCP_PROJECT", "my-gcp-project")
DATASET = os.environ.get("RTA_BQ_DATASET", "commerce_analytics")
LOCATION = os.environ.get("RTA_BQ_LOCATION", "US")
SQL_DIR = str(Path(__file__).parent / "sql")

with DAG(
    dag_id="daily_order_kpis",
    description="Roll streaming order events into daily KPIs with data-quality gates",
    schedule="15 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[SQL_DIR],
    user_defined_macros={"project": PROJECT, "dataset": DATASET},
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    doc_md=__doc__,
    tags=["commerce", "bigquery", "kpis"],
) as dag:
    check_source_freshness = BigQueryCheckOperator(
        task_id="check_source_freshness",
        sql="SELECT COUNT(*) > 0 FROM `{{ project }}.{{ dataset }}.order_events` WHERE DATE(occurred_at) = '{{ ds }}'",
        use_legacy_sql=False,
        location=LOCATION,
    )

    check_no_duplicate_events = BigQueryValueCheckOperator(
        task_id="check_no_duplicate_events",
        sql="duplicate_rate.sql",
        pass_value=0,
        tolerance=0.05,
        use_legacy_sql=False,
        location=LOCATION,
    )

    build_daily_kpis = BigQueryInsertJobOperator(
        task_id="build_daily_kpis",
        configuration={"query": {"query": "{% include 'daily_order_kpis.sql' %}", "useLegacySql": False}},
        location=LOCATION,
    )

    check_kpis_written = BigQueryCheckOperator(
        task_id="check_kpis_written",
        sql="SELECT COUNT(*) = 1 FROM `{{ project }}.{{ dataset }}.daily_order_kpis` WHERE kpi_date = '{{ ds }}'",
        use_legacy_sql=False,
        location=LOCATION,
    )

    check_success_rate_bounds = BigQueryCheckOperator(
        task_id="check_success_rate_bounds",
        sql=(
            "SELECT LOGICAL_AND(payment_success_rate IS NULL OR payment_success_rate BETWEEN 0 AND 1) "
            "FROM `{{ project }}.{{ dataset }}.daily_order_kpis` WHERE kpi_date = '{{ ds }}'"
        ),
        use_legacy_sql=False,
        location=LOCATION,
    )

    check_source_freshness >> [check_no_duplicate_events, build_daily_kpis]
    build_daily_kpis >> [check_kpis_written, check_success_rate_bounds]
