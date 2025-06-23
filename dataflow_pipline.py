import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.metrics.metric import Metrics, MetricsFilter
from apache_beam import DoFn, pvalue
from datetime import datetime
import re
import subprocess
import json

# ----------------------------------------
# 🧭 Timestamped run ID for temp/log buckets
# ----------------------------------------
run_id = datetime.utcnow().strftime("run-%Y%m%d-%H%M%S")

# ----------------------------------------
# ✅ Beam Metric Counters
# ----------------------------------------
null_customer_counter        = Metrics.counter("validation", "null_customers")
null_transaction_counter     = Metrics.counter("validation", "null_transactions")
negative_id_counter          = Metrics.counter("validation", "negative_transaction_or_customer_id")
negative_amount_counter      = Metrics.counter("validation", "negative_amount")
non_gbp_currency_counter     = Metrics.counter("validation", "non_gbp_currency")
invalid_date_counter         = Metrics.counter("validation", "invalid_date_format")

# ----------------------------------------
# 📋 BigQuery Schemas
# ----------------------------------------
CUSTOMER_SCHEMA = "CustomerID:INTEGER,Name:STRING,Email:STRING,Phone:STRING,City:STRING"
TRANSACTION_SCHEMA = "TransactionID:INTEGER,CustomerID:INTEGER,Amount:FLOAT,Currency:STRING,TransactionDate:DATE"

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")

# ----------------------------------------
# 🔍 Helper Functions
# ----------------------------------------
def has_nulls(fields):
    return any(f.strip() == "" for f in fields)

def validate_customer(record):
    try:
        fields = record.split(",")
        if len(fields) != 5 or has_nulls(fields):
            null_customer_counter.inc()
            return None

        customer_id = int(fields[0])
        name = fields[1]
        email = fields[2]
        phone = fields[3]
        city = fields[4]

        if customer_id <= 0 or not EMAIL_REGEX.match(email) or len(phone) < 10 or not city.strip():
            null_customer_counter.inc()
            return None

        return {
            "CustomerID": customer_id,
            "Name": name,
            "Email": email,
            "Phone": phone,
            "City": city
        }
    except Exception:
        null_customer_counter.inc()
        return None

def validate_transaction(record):
    try:
        fields = record.split(",")
        if len(fields) != 5 or has_nulls(fields):
            null_transaction_counter.inc()
            return None

        transaction_id = int(fields[0])
        customer_id = int(fields[1])
        amount = float(fields[2])
        currency = fields[3]
        date = fields[4]

        if transaction_id <= 0 or customer_id <= 0:
            negative_id_counter.inc()
            return None
        if amount <= 0:
            negative_amount_counter.inc()
            return None
        if currency != "GBP":
            non_gbp_currency_counter.inc()
            return None
        if not re.match(r"\d{4}-\d{2}-\d{2}", date):
            invalid_date_counter.inc()
            return None

        return {
            "TransactionID": transaction_id,
            "CustomerID": customer_id,
            "Amount": amount,
            "Currency": currency,
            "TransactionDate": date
        }
    except Exception:
        null_transaction_counter.inc()
        return None

# ----------------------------------------
# 🛠️ DoFn for flexible tagging
# ----------------------------------------
class ValidateAndTag(DoFn):
    def __init__(self, validator):
        self.validator = validator

    def process(self, element):
        result = self.validator(element)
        if result:
            yield result
        else:
            yield pvalue.TaggedOutput("invalids", {"raw_line": element})

# ----------------------------------------
# 🚀 Pipeline Options
# ----------------------------------------
options = PipelineOptions(
    runner="DataflowRunner",
    project="gcp-test-project-395421",
    region="europe-west1",
    temp_location=f"gs://poc-data-pipeline-bucket/data/tmp/{run_id}/",
    staging_location=f"gs://poc-data-pipeline-bucket/data/staging/{run_id}/",
    num_workers=1,
    max_num_workers=2
)

# ----------------------------------------
# 🛠️ Run Beam Pipeline
# ----------------------------------------
with beam.Pipeline(options=options) as p:
    # Customers
    customers = (
        p
        | "Read Customers" >> beam.io.ReadFromText("gs://poc-data-pipeline-bucket/data/customer.csv", skip_header_lines=1)
        | "Validate Customers" >> beam.ParDo(ValidateAndTag(validate_customer)).with_outputs("invalids", main="valids")
    )
    
    customers.valids | "Write Valid Customers" >> beam.io.WriteToBigQuery(
        "ecommerce.customer",
        schema=CUSTOMER_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    customers.invalids | "Write Invalid Customers" >> beam.io.WriteToBigQuery(
        "ecommerce.quarantine_customers",
        schema="raw_line:STRING",
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    # Transactions
    transactions = (
        p
        | "Read Transactions" >> beam.io.ReadFromText("gs://poc-data-pipeline-bucket/data/transactions.csv", skip_header_lines=1)
        | "Validate Transactions" >> beam.ParDo(ValidateAndTag(validate_transaction)).with_outputs("invalids", main="valids")
    )

    transactions.valids | "Write Valid Transactions" >> beam.io.WriteToBigQuery(
        "ecommerce.transactions",
        schema=TRANSACTION_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    transactions.invalids | "Write Invalid Transactions" >> beam.io.WriteToBigQuery(
        "ecommerce.quarantine_transactions",
        schema="raw_line:STRING",
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

# ----------------------------------------
# 📊 Metrics + Logging Summary
# ----------------------------------------
from apache_beam.runners.runner import PipelineResult

result = p.run()
result.wait_until_finish()

customer_quarantined = result.metrics().query(MetricsFilter().with_name("null_customers"))["counters"][0].committed
transaction_quarantined = result.metrics().query(MetricsFilter().with_name("null_transactions"))["counters"][0].committed

def log_pipeline_summary(run_id, customer_count, transaction_count):
    summary = {
        "run_id": run_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "quarantined_customers": customer_count,
        "quarantined_transactions": transaction_count
    }

    local_path = f"/tmp/summary_{run_id}.json"
    remote_path = f"gs://poc-data-pipeline-bucket/logs/summary_{run_id}.json"

    with open(local_path, "w") as f:
        json.dump(summary, f, indent=2)

    try:
        subprocess.run(["gsutil", "cp", local_path, remote_path], check=True)
        print(f"📝 Summary log uploaded to: {remote_path}")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Summary log upload failed: {e}")

log_pipeline_summary(run_id, customer_quarantined, transaction_quarantined)

# ----------------------------------------
# 🧹 Cleanup GCS Temp/Staging Folders
# ----------------------------------------
def clean_up_gcs_folders(bucket_paths):
    for path in bucket_paths:
        print(f"🧹 Cleaning up: {path}")
        try:
            subprocess.run(["gsutil", "-m", "rm", "-r", path], check=True)
            print(f"✅ Deleted: {path}")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Failed to delete: {path} – {e}")

cleanup_paths = [
    f"gs://poc-data-pipeline-bucket/data/tmp/{run_id}/",
    f"gs://poc-data-pipeline-bucket/staging/{run_id}/"
]

clean_up_gcs_folders(cleanup_paths)

print("✅ Pipeline with validation, logging, and cleanup successfully deployed!")


