import os
import datetime
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1
from google.protobuf.timestamp_pb2 import Timestamp

# Authenticate using service account
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "ticket-sauce-data-58c586aceed9.json"
client = bigquery_datatransfer_v1.DataTransferServiceClient()


def check_job_status(job_id):
    job = client.get_job(job_id)
    print(f"Job ID: {job.job_id}")
    print(f"Job State: {job.state}")

    # Check if job is done
    if job.state == 'DONE':
        if job.error_result:
            print("Job failed with error:", job.error_result)
            return False
        else:
            print("Job completed successfully.")
            return True
    else:
        print("Job is still running...")
        return False


def trigger_query(transfer_config_name):
    # Get current UTC time as a Timestamp
    now = datetime.datetime.utcnow()
    timestamp = Timestamp()
    timestamp.FromDatetime(now)

    # Start the transfer run
    response = client.start_manual_transfer_runs(
        {
            "parent": transfer_config_name,
            "requested_run_time": timestamp,
        }
    )
    # Print results
    for run in response.runs:
        print(f"Started manual transfer run: {run.name}")
        job_id = run.name.split("/")[-1]
        print(check_job_status(job_id))

def shami_test_schedule():
    # Transfer config resource name
    transfer_config_name = "projects/266163061237/locations/us/transferConfigs/681f53f4-0000-2cfc-bda1-883d24f4f6c4"
    trigger_query(transfer_config_name)
