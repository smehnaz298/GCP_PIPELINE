import os
import json
import traceback
from googleapiclient.discovery import build

def launch_dataflow(event, context):
    try:
        file_name = event['name']
        bucket = event['bucket']

        if not file_name.endswith('.csv'):
            print("⛔ Skipping non-CSV file.")
            return

        print(f"📂 Detected file: gs://{bucket}/{file_name}")

        project = os.environ['GCP_PROJECT']
        region = os.environ['REGION']
        template_path = os.environ['TEMPLATE_SPEC_PATH']
        job_name = f"flex-pipeline-{context.event_id[:8]}"

        print(f"🚀 Starting Dataflow job '{job_name}' using template: {template_path}")

        # Clean and print the schema path
        schema_path = "gs://poc-data-pipeline-bucket/schemas/customer_schema.json".strip().replace('\n', '').replace('\r', '')
        print(f"📎 Schema path repr: {repr(schema_path)}")

        # Build launch body
        launch_body = {
            "launchParameter": {
                "jobName": job_name,
                "containerSpecGcsPath": template_path,
                "parameters": {
                    "inputFileSpec": f"gs://{bucket}/{file_name}",
                    "inputFileFormat": "csv",
                    "outputFileFormat": "parquet",
                    "outputBucket": f"gs://{bucket}/output/",
                    "schema": schema_path
                }
            }
        }

        print("🧾 Launch request body:")
        print(json.dumps(launch_body, indent=2))

        dataflow = build('dataflow', 'v1b3')
        response = dataflow.projects().locations().flexTemplates().launch(
            projectId=project,
            location=region,
            body=launch_body
        ).execute()

        print(f"✅ Dataflow job launched: {response.get('job', {}).get('name')}")
        print("🧾 Full API response:")
        print(json.dumps(response, indent=2))
        print(f"🧪 Final schema URI: {repr(schema_path)}")

    except Exception as e:
        print("🔥 Error launching Dataflow job:")
        if hasattr(e, 'content'):
            print("💥 Full API Error Response:")
            print(e.content.decode())
        else:
            print(str(e))
        traceback.print_exc()
