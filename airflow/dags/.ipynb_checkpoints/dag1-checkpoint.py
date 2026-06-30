from admin import Admin
from producer import BinanceProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging


BOOTSTRAP_SERVERS = "ed-kafka-1:29092,ed-kafka-2:29093,ed-kafka-3:29094"
TOPIC = "binance"


def add_topic(ti):
    admin = Admin(bootstrapserver=BOOTSTRAP_SERVERS)
    try:
        admin.create_topic(topic=TOPIC, partition=3)
        ti.xcom_push(key="bootstrapserver", value=BOOTSTRAP_SERVERS)
        ti.xcom_push(key="topic", value=TOPIC)
        logging.info(f"Topic {TOPIC} created successfully")
    except Exception as e:
        ti.xcom_push(key="bootstrapserver", value=BOOTSTRAP_SERVERS)
        ti.xcom_push(key="topic", value=TOPIC)
        logging.error(f"ERROR Creating Topic {TOPIC}: {str(e)}")


def send_message(ti):
    bootstrapserver = ti.xcom_pull(key="bootstrapserver", task_ids="create_topic")
    topic = ti.xcom_pull(key="topic", task_ids="create_topic")
    producer = BinanceProducer(
        bootstrapserver,
        topic,
        4 * 1024,
        "snappy",
        batch_size=10000,
        waiting_time=100
    )
    try:
        prices = producer.send_message()
        if prices:
            ti.xcom_push(key="send_message", value=prices)
        else:
            ti.xcom_push(key="send_message", value="ERROR: no prices returned")
    except Exception as e:
        ti.xcom_push(key="send_message", value=f"ERROR - {str(e)}")
        logging.error(f"ERROR sending message: {str(e)}")


default_args = {
    "owner": "Abdellah",
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30)
}

with DAG(
    dag_id="BinanceStreaming",
    description="Streaming Binance Data",
    default_args=default_args,
    schedule=timedelta(seconds=10),
    catchup=False,
    tags=["kafka", "spark", "airflow"]
) as dag:

    create_topic = PythonOperator(
        task_id="create_topic",
        python_callable=add_topic
    )

    send_prices = PythonOperator(
        task_id="send_price",
        python_callable=send_message
    )

    create_topic >> send_prices