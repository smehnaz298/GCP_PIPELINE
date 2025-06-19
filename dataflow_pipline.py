import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re

# Define BigQuery Schema
CUSTOMER_SCHEMA = "CustomerID:INTEGER,Name:STRING,Email:STRING,Phone:STRING,City:STRING"
TRANSACTION_SCHEMA = "TransactionID:INTEGER,CustomerID:INTEGER,Amount:FLOAT,Currency:STRING,Date:STRING"

# Email Validation Regex
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")

def has_nulls(fields):
    """ Returns True if any field is empty or whitespace-only """
    return any(f.strip() == "" for f in fields)


# Data Validation & Cleansing Functions
def validate_customer(record):
    """ Validate & Clean Customer Data """
    try:
        fields = record.split(",")
        if len(fields) != 5 or has_nulls(fields):
            return None  # Invalid row

        
        customer_id = int(fields[0])
        email = fields[2]
        phone = fields[3]
        city = fields[4]

        # Validation checks
        if customer_id <= 0:
            return None
        if not EMAIL_REGEX.match(email):
            return None
        if len(phone) < 10:
            return None
        if not city.strip():
            return None

        return {
            "CustomerID": customer_id,
            "Name": fields[1],
            "Email": email,
            "Phone": phone,
            "City": city
        }
    except Exception:
        return None  # Error handling

def validate_transaction(record):
    """ Validate & Clean Transaction Data """
    try:
        fields = record.split(",")
        if len(fields) != 5:
            return None  # Invalid row

        transaction_id = int(fields[0])
        customer_id = int(fields[1])
        amount = float(fields[2])
        currency = fields[3]
        date = fields[4]

        # Validation checks
        if transaction_id <= 0 or customer_id <= 0:
            return None
        if amount <= 0:
            return None
        if currency != "GBP":  # Extend validation if needed
            return None
        if not re.match(r"\d{4}-\d{2}-\d{2}", date):  # Validate Date format (YYYY-MM-DD)
            return None
        if has_nulls(fields):
            return None


        return {
            "TransactionID": transaction_id,
            "CustomerID": customer_id,
            "Amount": amount,
            "Currency": currency,
            "Date": date
        }
    except Exception:
        return None  # Error handling

# Pipeline Options
options = PipelineOptions(
    runner="DataflowRunner",
    project="gcp-test-project-395421",
    temp_location="gs://poc-data-pipeline-bucket",
    region="europe-west1"
)

# Apache Beam Pipeline with Data Validation
with beam.Pipeline(options=options) as p:
    # Read and validate customer data
    customers = (
        p
        | "Read Customers" >> beam.io.ReadFromText("gs://poc-data-pipeline-bucket/customer.csv", skip_header_lines=1)
        | "Validate Customers" >> beam.Map(validate_customer)
        | "Filter Invalid Customers" >> beam.Filter(lambda x: x is not None)
        | "Write Customers to BigQuery" >> beam.io.WriteToBigQuery(
            "ecommerce.customer",
            schema=CUSTOMER_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )

    # Read and validate transaction data
    transactions = (
        p
        | "Read Transactions" >> beam.io.ReadFromText("gs://poc-data-pipeline-bucket/transactions.csv", skip_header_lines=1)
        | "Validate Transactions" >> beam.Map(validate_transaction)
        | "Filter Invalid Transactions" >> beam.Filter(lambda x: x is not None)
        | "Write Transactions to BigQuery" >> beam.io.WriteToBigQuery(
            "ecommerce.transactions",
            schema=TRANSACTION_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )

print("Pipeline with data validation successfully deployed!")

