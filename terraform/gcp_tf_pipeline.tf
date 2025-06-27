provider "google" {
  project = "gcp-test-project-395421"
  region  = "europe-west1"
}

# GCS Bucket
resource "google_storage_bucket" "pipeline" {
  name     = "poc-data-pipeline-bucket_tf"
  location = "EU"
  force_destroy = true
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 2
    }
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "ecommerce_tf" {
  dataset_id                  = "ecommerce_tf"
  location                    = "EU"
  delete_contents_on_destroy = true
  labels = {
    environment = "dev"
    owner       = "data-pipeline"
  }
}

# Customer Table
resource "google_bigquery_table" "customers_tf" {
  dataset_id = google_bigquery_dataset.ecommerce_tf.dataset_id
  table_id   = "customer_tf"

  schema = jsonencode([
    { name = "CustomerID", type = "INTEGER", mode = "REQUIRED" },
    { name = "Name",       type = "STRING",  mode = "NULLABLE" },
    { name = "Email",      type = "STRING",  mode = "NULLABLE" },
    { name = "Phone",      type = "STRING",  mode = "NULLABLE" },
    { name = "City",       type = "STRING",  mode = "NULLABLE" }
  ])
}

# Transaction Table
resource "google_bigquery_table" "transactions_tf" {
  dataset_id = google_bigquery_dataset.ecommerce_tf.dataset_id
  table_id   = "transactions_tf"

  schema = jsonencode([
    { name = "TransactionID",   type = "INTEGER", mode = "REQUIRED" },
    { name = "CustomerID",      type = "INTEGER", mode = "REQUIRED" },
    { name = "Amount",          type = "FLOAT",   mode = "NULLABLE" },
    { name = "Currency",        type = "STRING",  mode = "NULLABLE" },
    { name = "TransactionDate", type = "DATE",    mode = "NULLABLE" }
  ])
}

# Quarantine Tables
resource "google_bigquery_table" "quarantine_customers_tf" {
  dataset_id = google_bigquery_dataset.ecommerce_tf.dataset_id
  table_id   = "quarantine_customers_tf"

  schema = jsonencode([
    { name = "raw_line", type = "STRING", mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "quarantine_transactions_tf" {
  dataset_id = google_bigquery_dataset.ecommerce_tf.dataset_id
  table_id   = "quarantine_transactions_tf"

  schema = jsonencode([
    { name = "raw_line", type = "STRING", mode = "NULLABLE" }
  ])
}

# Upload Cloud Function code to GCS
resource "google_storage_bucket_object" "function_code" {
  name   = "cloud-function.zip"
  bucket = "poc-data-pipeline-bucket"
  source = "cloud-function.zip"
}

# Cloud Function to trigger Dataflow Flex Template
resource "google_cloudfunctions_function" "dataflow_trigger" {
  name        = "dataflow-csv-trigger"
  runtime     = "python312"
  entry_point = "launch_dataflow"

  source_archive_bucket = "poc-data-pipeline-bucket"
  source_archive_object = "cloud-function.zip"
  available_memory_mb   = 256
  timeout               = 60

  environment_variables = {
    GCP_PROJECT        = "gcp-test-project-395421"
    REGION             = "europe-west1"
    TEMPLATE_SPEC_PATH = "gs://dataflow-templates/latest/flex/File_Format_Conversion"
  }

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = "poc-data-pipeline-bucket_tf"
    resource   = "poc-data-pipeline-bucket"
    failure_policy {
      retry = true
    }
  }
}

# =====================
# IAM ROLE BINDINGS
# =====================

# Service account deployed with Dataflow
variable "dataflow_service_account" {
  default = "dataflow-sa@gcp-test-project-395421.iam.gserviceaccount.com"
}

# Allow the Dataflow service account to run jobs
resource "google_project_iam_member" "dataflow_worker" {
  project = "gcp-test-project-395421"
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${var.dataflow_service_account}"
}

# BigQuery permissions
resource "google_project_iam_member" "bq_editor" {
  project = "gcp-test-project-395421"
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${var.dataflow_service_account}"
}

# GCS permissions
resource "google_project_iam_member" "storage_admin" {
  project = "gcp-test-project-395421"
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${var.dataflow_service_account}"
}

