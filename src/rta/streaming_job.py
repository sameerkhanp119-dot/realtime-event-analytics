"""Spark Structured Streaming job: Kafka -> raw events + per-minute KPIs.

Sinks (SINK env var):
  console   print micro-batches (local demo)
  parquet   append to OUTPUT_PATH/<table>
  bigquery  append to <BQ_PROJECT>.<BQ_DATASET>.<table> via the Spark BigQuery connector

Delivery is at-least-once; the daily Airflow rollup de-duplicates raw events by event_id.
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from rta.events import ORDER_EVENTS_TOPIC, PAYMENT_EVENTS_TOPIC
from rta.transforms import EVENT_COLUMNS, deduplicate, minute_kpis, parse_events

log = logging.getLogger("rta.streaming")


def _batch_writer(table: str):
    sink = os.environ.get("SINK", "console")

    def write(batch: DataFrame, batch_id: int) -> None:
        if sink == "bigquery":
            target = f"{os.environ['BQ_PROJECT']}.{os.environ['BQ_DATASET']}.{table}"
            batch.write.format("bigquery").option("table", target).option("writeMethod", "direct").mode("append").save()
        elif sink == "parquet":
            batch.write.mode("append").parquet(os.path.join(os.environ.get("OUTPUT_PATH", "/tmp/rta"), table))
        else:
            batch.orderBy(batch.columns[0]).show(20, truncate=False)
        log.info("batch %s written to %s (%s)", batch_id, table, sink)

    return write


def build(spark: SparkSession) -> list:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
        .option("subscribe", f"{ORDER_EVENTS_TOPIC},{PAYMENT_EVENTS_TOPIC}")
        .option("startingOffsets", os.environ.get("STARTING_OFFSETS", "latest"))
        .option("includeHeaders", "true")
        .option("maxOffsetsPerTrigger", os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))
        .load()
    )
    events = deduplicate(parse_events(raw))
    checkpoints = os.environ.get("CHECKPOINT_PATH", "/tmp/rta-checkpoints")
    trigger = os.environ.get("TRIGGER_INTERVAL", "30 seconds")

    raw_query = (
        events.select(*EVENT_COLUMNS).withColumn("ingested_at", F.current_timestamp())
        .writeStream.queryName("order_events")
        .foreachBatch(_batch_writer("order_events"))
        .option("checkpointLocation", f"{checkpoints}/order_events")
        .trigger(processingTime=trigger)
        .start()
    )
    kpi_query = (
        minute_kpis(events)
        .writeStream.queryName("order_kpis_minute")
        .outputMode("append")
        .foreachBatch(_batch_writer("order_kpis_minute"))
        .option("checkpointLocation", f"{checkpoints}/order_kpis_minute")
        .trigger(processingTime=trigger)
        .start()
    )
    return [raw_query, kpi_query]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = (
        SparkSession.builder.appName("realtime-event-analytics")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )
    build(spark)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
