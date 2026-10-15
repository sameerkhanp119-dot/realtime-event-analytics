output "dataset" {
  description = "Fully qualified BigQuery dataset"
  value       = "${var.project_id}.${google_bigquery_dataset.analytics.dataset_id}"
}

output "pipeline_bucket" {
  description = "Bucket for Spark checkpoints (gs://.../checkpoints) and staging"
  value       = google_storage_bucket.pipeline.url
}

output "streaming_service_account" {
  description = "Service account the streaming job runs as"
  value       = google_service_account.streaming.email
}
