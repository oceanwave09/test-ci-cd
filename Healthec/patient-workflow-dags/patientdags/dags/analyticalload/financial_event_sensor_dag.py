import csv
import json
import logging
import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaError, KafkaException
from smart_open import open

from patientdags.utils.constants import EVENT_FIELDS

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(day=2, year=2022, month=8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=1)
}


def _init_dag(ti):
    # dag start timestamp
    start_ts = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d-%H-%M-%S")
    # event file path for this run
    event_file_path = os.path.join(
        f"s3://{Variable.get('DATA_INGESTION_BUCKET')}/",
        "files/ANALYTICAL/FINANCIAL/landing/_TENANT_",
        f'financial_topic_events_{datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M_%S")}.csv',
    )
    ti.xcom_push(key="start_ts", value=start_ts)
    ti.xcom_push(key="event_file_path", value=event_file_path)


def _write_events_into_s3(events: list, file_path: str):
    with open(file_path, "w") as event_file:
        event_writer = csv.DictWriter(event_file, fieldnames=EVENT_FIELDS)
        event_writer.writeheader()
        event_writer.writerows(events)


def _consume_events(start_ts: str, event_file_path: str):
    start_time = datetime.strptime(start_ts, "%Y-%m-%d-%H-%M-%S")
    # read kafka financial topic variables
    servers = Variable.get("KAFKA_SERVERS")
    topic = Variable.get("FINANCIAL_TOPIC_NAME")
    max_batch_messages = int(Variable.get("KAFKA_MAX_BATCH_MESSAGES"))
    await_timeout = int(Variable.get("KAFKA_AWAIT_TIMEOUT"))
    logging.info(
        f"server: {servers}, topic: {topic}, max_batch_messages: {max_batch_messages}, "
        f"await_timeout: {await_timeout}"
    )

    # kafka consumer configuration
    logging.info("Configuring consumer")
    conf = {
        "bootstrap.servers": servers,
        "group.id": "analytical_load_financial_group",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    # kafka consumer
    consumer = Consumer(conf)
    events_by_tenant = {}
    event_count = 0
    read_msg_count = 0

    try:
        # collects events from kafka topic
        consumer.subscribe([topic])
        logging.info("Start polling messages from topic")
        while event_count < max_batch_messages:
            delta = datetime.now(timezone.utc).replace(tzinfo=None) - start_time
            if int(delta.seconds) >= await_timeout:
                logging.info("Await timeout reached, stop polling messages.")
                break
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                logging.info("No message to receive, stop polling messages.")
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.error("%% %s [%d] reached end at offset %d\n" % (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                data = msg.value().decode("utf-8")
                logging.info(f"Received message: {data}")
                json_data = json.loads(data)
                read_msg_count = read_msg_count + 1

                # filter only `create`, `update` events
                if json_data.get("event", "") in ["create", "update"]:
                    tenant = json_data.get("tenantId")
                    if events_by_tenant.get(tenant):
                        events_by_tenant[tenant].append(json_data)
                    else:
                        events_by_tenant[tenant] = [json_data]
                    event_count = event_count + 1

        # write events as CSV file into S3 bucket analytical directory if events exists in the list
        if len(events_by_tenant) > 0:
            for tenant, events in events_by_tenant.items():
                tenant_event_file_path = event_file_path.replace("_TENANT_", tenant)
                _write_events_into_s3(events, tenant_event_file_path)

        # commit kafka topic offset for group
        if read_msg_count > 0:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()


with DAG(
    dag_id="financial_event_sensor_dag",
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    max_active_runs=1,
    catchup=False,
    description=(
        "Runs for every 30 minutes, collects financial events from financial kafka topic and "
        "writes events as CSV file into S3 bucket analytical directory"
    ),
    render_template_as_native_obj=True,
    tags=["sensor", "kafka", "financial"],
) as dag:
    init_dag = PythonOperator(task_id="init_dag", python_callable=_init_dag)

    event_listener = PythonOperator(
        task_id="event_listener",
        python_callable=_consume_events,
        op_kwargs={
            "start_ts": '{{ ti.xcom_pull(task_ids="init_dag", key="start_ts") }}',
            "event_file_path": '{{ ti.xcom_pull(task_ids="init_dag", key="event_file_path") }}',
        },
    )

    init_dag >> event_listener
