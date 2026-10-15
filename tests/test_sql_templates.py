from pathlib import Path

import jinja2
import pytest

SQL_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "sql"
CONTEXT = {"project": "proj", "dataset": "ds_analytics", "ds": "2026-10-14"}


@pytest.mark.parametrize("name", sorted(p.name for p in SQL_DIR.glob("*.sql")))
def test_sql_renders_with_airflow_macros(name):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(SQL_DIR), undefined=jinja2.StrictUndefined)
    rendered = env.get_template(name).render(**CONTEXT)

    assert "{{" not in rendered and "}}" not in rendered
    assert "`proj.ds_analytics.order_events`" in rendered
    assert "'2026-10-14'" in rendered


def test_rollup_is_an_idempotent_merge():
    sql = (SQL_DIR / "daily_order_kpis.sql").read_text()
    assert sql.lstrip("-").strip().splitlines()[-1]  # not empty
    assert "MERGE" in sql and "WHEN MATCHED THEN UPDATE" in sql and "ROW_NUMBER()" in sql
