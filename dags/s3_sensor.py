import os
from datetime import datetime, timedelta

import airflow.utils.dates
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor
from plugins.operators.dag_s3_to_postgres import S3ToPostgresTransferFromXcom
from plugins.operators.s3_get_keys import S3ListKeys
from plugins.operators.slack_notifications import NotifySlackDagResult

NEW_FILES_PREFIX = os.environ.get("NEW_FILES_PREFIX")
PROCESSED_FILES_PREFIX = os.environ.get("PROCESSED_FILES_PREFIX")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
POSTGRES_TABLE = os.environ.get("POSTGRES_TABLE")
POSTGRES_SCHEMA = os.environ.get("POSTGRES_SCHEMA")
SLACK_HTTP_CONNECTION_ID = os.environ.get("SLACK_HTTP_CONNECTION_ID")
SLACK_WEBHOOK_TOKEN = os.environ.get("SLACK_WEBHOOK_TOKEN")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL")
DAG_NAME = os.environ.get("S3_SENSOR_DAG_NAME")

yday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())


default_args = {
    "owner": "jose.michel",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
}

with DAG(DAG_NAME, default_args=default_args, schedule_interval="@daily") as dag:
    check_for_prefix = S3PrefixSensor(
        task_id="s3_prefix_sensor",
        poke_interval=0,
        timeout=10,
        soft_fail=True,
        prefix=NEW_FILES_PREFIX,
        bucket_name=BUCKET_NAME,
    )

    list_s3_keys = S3ListKeys(
        task_id="list_s3_keys",
        bucket_name=BUCKET_NAME,
        prefix=NEW_FILES_PREFIX,
        track_status=True,
    )

    load_user_purchases = S3ToPostgresTransferFromXcom(
        task_id="load_user_purchases_to_postgre",
        pull_from_tasks=["list_s3_keys"],
        schema=POSTGRES_SCHEMA,
        table=POSTGRES_TABLE,
        bucket_name=BUCKET_NAME,
        dest_prefix=PROCESSED_FILES_PREFIX,
        track_status=True,
    )

    def get_success_message():
        return "Module2: Challenge yourself - Integrate Slack with Airflow (message sent by Gabriel). DAG success"

    def get_fail_message():
        return "There was an error excecuting your DAG"

    send_slack_notification = NotifySlackDagResult(
        task_id="send_slack_message_challenge",
        http_conn_id=SLACK_HTTP_CONNECTION_ID,
        message_success=get_success_message(),
        message_failure=get_fail_message(),
        channel=SLACK_CHANNEL,
        trigger_rule="all_done",
    )

    (check_for_prefix >> list_s3_keys >> load_user_purchases >> send_slack_notification)
