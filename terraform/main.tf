resource "google_bigquery_dataset" "analytics" {
  dataset_id                 = var.dataset_id
  location                   = var.bq_location
  description                = "Commerce order and payment events with streaming and daily KPIs"
  delete_contents_on_destroy = false

  labels = {
    app        = "realtime-event-analytics"
    managed_by = "terraform"
  }
}

# Raw events: partitioned by event day so the daily rollup scans one partition,
# clustered for per-order lookups.
resource "google_bigquery_table" "order_events" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "order_events"
  description         = "Every order and payment event from Kafka (at-least-once; deduplicate by event_id)"
  clustering          = ["event_type", "order_id"]
  schema              = file("${path.module}/schemas/order_events.json")
  deletion_protection = true

  time_partitioning {
    type          = "DAY"
    field         = "occurred_at"
    expiration_ms = var.raw_event_retention_days * 24 * 60 * 60 * 1000
  }
}

resource "google_bigquery_table" "order_kpis_minute" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "order_kpis_minute"
  description         = "Per-minute KPIs emitted by the streaming job"
  schema              = file("${path.module}/schemas/order_kpis_minute.json")
  deletion_protection = true

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }
}

resource "google_bigquery_table" "daily_order_kpis" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "daily_order_kpis"
  description         = "Daily rollup built by the Airflow DAG daily_order_kpis"
  schema              = file("${path.module}/schemas/daily_order_kpis.json")
  deletion_protection = true

  time_partitioning {
    type  = "MONTH"
    field = "kpi_date"
  }
}

# Spark checkpoints and connector staging files.
resource "google_storage_bucket" "pipeline" {
  name                        = "${var.project_id}-rta-pipeline"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  lifecycle_rule {
    condition {
      age            = 7
      matches_prefix = ["tmp/"]
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_service_account" "streaming" {
  account_id   = "rta-streaming"
  display_name = "Realtime event analytics streaming job"
}

resource "google_bigquery_dataset_iam_member" "streaming_writer" {
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.streaming.email}"
}

resource "google_project_iam_member" "streaming_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.streaming.email}"
}

resource "google_storage_bucket_iam_member" "streaming_bucket" {
  bucket = google_storage_bucket.pipeline.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.streaming.email}"
}
