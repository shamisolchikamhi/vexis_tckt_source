import os
import time

from google.oauth2 import service_account
from google.auth import default
import datetime
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1
from google.protobuf.timestamp_pb2 import Timestamp
from google.cloud import pubsub_v1
import json

# Authenticate using service account
project_id = "ticket-sauce-data"
try:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "ticket-sauce-data-58c586aceed9.json"
except:
    pass

transfer_client = bigquery_datatransfer_v1.DataTransferServiceClient()
bq_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()

def pub_sub_message_publisher(project_id, topic, message):
    topic_path = publisher.topic_path(project_id, topic)
    new_message = {
        'result': message
    }
    future = publisher.publish(topic_path, json.dumps(new_message).encode('utf-8'))
    print(future.result(timeout=30))

def get_job_results(job_id):
    # Fetch the job
    job = bq_client.get_job(job_id=f'scheduled_query_{job_id}')
    result = job.result()
    return result

def trigger_query(transfer_config_name):
    # Get current UTC time as a Timestamp
    now = datetime.datetime.utcnow()
    timestamp = Timestamp()
    timestamp.FromDatetime(now)

    # Start the transfer run
    response = transfer_client.start_manual_transfer_runs(
        {
            "parent": f'projects/266163061237/locations/us/transferConfigs/{transfer_config_name}',
            "requested_run_time": timestamp,
        }
    )
    for run in response.runs:
        job_id = run.name.split("/")[-1]
        time.sleep(60)
        output = get_job_results(job_id)
    return f"Job {job_id} completed, {output}."


def shami_test_schedule(event, context):
    # Transfer config resource name
    transfer_config_name = "681f53f4-0000-2cfc-bda1-883d24f4f6c4"
    output = trigger_query(transfer_config_name)
    pub_sub_message_publisher(project_id = project_id, topic = "shami_test_schedule",
                              message= output)

def main_query(event, context):
    # Main Query - Update Master Sheet
    transfer_config_name = "65ffc7e2-0000-2595-97f7-240588844730"
    output = trigger_query(transfer_config_name)
    pub_sub_message_publisher(project_id=project_id, topic="main_query",
                              message=output)

def weekly_event_tickets(event, context):
    transfer_config_name = "6615adbd-0000-28e8-ac41-14c14ef12150"
    output = trigger_query(transfer_config_name)
    pub_sub_message_publisher(project_id=project_id, topic="weekly_event_tickets",
                              message=output)

def ticket_tier_grouped(event, context):
    transfer_config_name = "660f30ba-0000-2c3e-b580-c82add7c7074"
    output = trigger_query(transfer_config_name)
    pub_sub_message_publisher(project_id=project_id, topic="ticket_tier_grouped",
                              message=output)

def days_to_event_sales(event, context):
    transfer_config_name = "665c5e27-0000-2bf5-bb38-582429a92e28"
    output = trigger_query(transfer_config_name)
    pub_sub_message_publisher(project_id=project_id, topic="days_to_event_sales",
                              message=output)

def tickets_sales_last_10_days_by_gender(event, context):
    transfer_config_name = "66396dcd-0000-2b01-9122-582429abfa20"
    output = trigger_query(transfer_config_name)
    pub_sub_message_publisher(project_id=project_id, topic="tickets_sales_last_10_days_by_gender",
                              message=output)

def ticket_revenue_report(event, context):
    transfer_config_name = "66321e40-0000-2c4b-9432-089e082d15a0"
    output = trigger_query(transfer_config_name)
    pub_sub_message_publisher(project_id=project_id, topic="ticket_revenue_report",
                              message=output)

def update_time(event, context):
    transfer_config_name = "6610964c-0000-2215-a2da-001a11453510"
    output = trigger_query(transfer_config_name)
    pub_sub_message_publisher(project_id=project_id, topic="update_time",
                              message=output)

def line_item_fee_name_types(event, context):
    transfer_config_name = "663d04ac-0000-25e4-8b83-ac3eb1468444"
    output = trigger_query(transfer_config_name)
    pub_sub_message_publisher(project_id=project_id, topic="line_item_fee_name_types",
                              message=output)

def log_for_comprehensive_ticket_table(event, context):
    transfer_config_name = "66172a53-0000-2a76-afb7-ac3eb146e5ec"
    output = trigger_query(transfer_config_name)
    pub_sub_message_publisher(project_id=project_id, topic="log_for_comprehensive_ticket_table",
                              message=output)
