"""Validates the Airflow DAG structure. Runs in the CI job that installs Airflow; skipped elsewhere."""
from pathlib import Path

import pytest

# The repo's airflow/ folder is importable as a namespace package, so probe a real Airflow module.
pytest.importorskip("airflow.models")

from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("daily_order_kpis")


def test_dag_is_configured_for_safe_scheduling(dag):
    assert dag is not None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.default_args["retries"] == 2


def test_task_graph(dag):
    assert set(dag.task_ids) == {
        "check_source_freshness",
        "check_no_duplicate_events",
        "build_daily_kpis",
        "check_kpis_written",
        "check_success_rate_bounds",
    }
    assert dag.get_task("build_daily_kpis").upstream_task_ids == {"check_source_freshness"}
    assert dag.get_task("check_kpis_written").upstream_task_ids == {"build_daily_kpis"}
    assert dag.get_task("check_success_rate_bounds").upstream_task_ids == {"build_daily_kpis"}
