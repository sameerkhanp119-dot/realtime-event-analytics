variable "project_id" {
  description = "GCP project that hosts the pipeline"
  type        = string
}

variable "region" {
  description = "Region for the bucket and compute"
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "dataset_id" {
  description = "BigQuery dataset for events and KPIs"
  type        = string
  default     = "commerce_analytics"
}

variable "raw_event_retention_days" {
  description = "How long raw event partitions are kept"
  type        = number
  default     = 400
}
