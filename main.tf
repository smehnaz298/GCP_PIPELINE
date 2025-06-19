provider "google" {
  project = "gcp-test-project-395421"
  region  = "europe-west1"
}

resource "google_storage_bucket" "dataflow_bucket" {
  name     = "poc-data-pipeline-bucket_terraform"
  location = "EU"
}

resource "google_bigquery_dataset" "terraform_dataset" {
  dataset_id = "terraform"
  location   = "EU"
}

resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-runner-sa"
  display_name = "Apache Beam Dataflow Service Account"
}

resource "google_project_iam_member" "dataflow_sa_roles" {
  for_each = toset([
    "roles/dataflow.admin",
    "roles/bigquery.dataEditor",
    "roles/storage.objectAdmin"
  ])
  role   = each.value
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

