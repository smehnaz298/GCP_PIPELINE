# GCP_PIPELINE

# 🚀 Serverless CSV-to-Parquet Pipeline on GCP

This project automates the conversion of `.csv` files to `.parquet` format using Google Cloud services including Cloud Functions, Dataflow Flex Templates, and GCS. Lightweight, scalable, and ideal for production pipelines.

## 📐 Architecture Overview

┌──────────────┐ │ GCS Bucket │ ← Upload CSV └────┬─────────┘ │ ▼ ┌───────────────┐ │ Cloud Function│ (Triggered on new file) └────┬──────────┘ │ ▼ ┌────────────┐ │ Dataflow │ ← Flex Template └────┬───────┘ │ ▼ ┌──────────────┐ │ GCS Output │ → Parquet in /output/ └──────────────┘


## 🔧 Setup Instructions

### Prerequisites

Enable the following APIs:
```bash
gcloud services enable \
  storage.googleapis.com \
  dataflow.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  iam.googleapis.com

## Grant your service account the following roles:


Storage Admin

Dataflow Admin

Cloud Functions Developer

Service Account User

Logging Writer


## GCS Structure

gsutil mb -l europe-west1 gs://poc-data-pipeline-bucket/
gsutil mkdir gs://poc-data-pipeline-bucket/output/
gsutil mkdir gs://poc-data-pipeline-bucket/schemas/

## Schema File
Upload a schema JSON file like:

[
  { "name": "customer_id", "type": "INT64" },
  { "name": "name", "type": "STRING" },
  { "name": "email", "type": "STRING" },
  { "name": "phone", "type": "STRING" },
  { "name": "city", "type": "STRING" }
]

## Upload the file

gsutil cp customer_schema.json gs://poc-data-pipeline-bucket/schemas/

## DEPLOY THE CLOUD FUNCTION

gcloud functions deploy dataflow-csv-trigger \
  --runtime python312 \
  --entry-point launch_dataflow \
  --trigger-resource poc-data-pipeline-bucket \
  --trigger-event google.storage.object.finalize \
  --region europe-west1 \
  --source ./cloud-function \
  --set-env-vars GCP_PROJECT=[your-project-id],REGION=europe-west1,TEMPLATE_SPEC_PATH=gs://dataflow-templates/latest/flex/File_Format_Conversion \
  --memory 512MB \
  --timeout 60s

## Terraform Support
If managing infrastructure via code, define variables such as:

project_id         = "your-project-id"
region             = "europe-west1"
bucket_name        = "poc-data-pipeline-bucket"
schema_gcs_path    = "gs://poc-data-pipeline-bucket/schemas/customer_schema.json"
function_name      = "dataflow-csv-trigger"

## INTIALISE AND RUN TERRAFORM

terraform init
terraform apply

## How to Use

Drop a .csv file into the root of gs://poc-data-pipeline-bucket/

Cloud Function triggers

Dataflow launches using File_Format_Conversion Flex Template

Parquet file is written to /output/ path in the same bucket

## Assumptions
CSV input contains headers

Schema is stored and accessible in GCS

Template supports csv → parquet conversion

Function runs in europe-west1

🌱 Future Improvements
✨ Add retry logic and exponential backoff on failure

✉️ Alerting via Teams/email on job outcome

🔍 Auto-validate CSV vs schema prior to launch

📊 Auto-load Parquet to BigQuery

🔐 Enforce least-privilege IAM bindings

🛠 GitHub Actions pipeline for CI/CD deployment




