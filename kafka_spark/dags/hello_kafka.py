from airflow import DAG
import datetime
import logging
import json
from airflow_provider_kafka.operators.await_message import AwaitKafkaMessageOperator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

default_args={
    'start_date':datetime.datetime(2022,1,1)
}

def producer_function():
    for i in range(20):
        yield (json.dumps(i), json.dumps(i + 1))



consumer_logger = logging.getLogger("airflow")
def consumer_function(message, prefix=None):
    key = json.loads(message.key())
    value = json.loads(message.value())
    consumer_logger.info(f"{prefix} {message.topic()} @ {message.offset()}; {key} : {value}")
    return


def await_function(message):
    if json.loads(message.value()) % 5 == 0:
        return f" Got the following message: {json.loads(message.value())}"

with DAG('hello_kafka',default_args=default_args,catchup=False,schedule_interval='@once') as dags:

    t1 = ProduceToTopicOperator(
        task_id="produce_to_topic",
        topic="test_1",
        producer_function="hello_kafka.producer_function",
        kafka_config={"bootstrap.servers": "broker:29092"},
    )

    t2 = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["test_1"],
        apply_function="hello_kafka.consumer_function",
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": "broker:29092",
            "group.id": "foo",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )

    t3 = AwaitKafkaMessageOperator(
        task_id="awaiting_message",
        topics=["test_1"],
        apply_function="hello_kafka.await_function",
        kafka_config={
            "bootstrap.servers": "broker:29092",
            "group.id": "awaiting_message",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        xcom_push_key="retrieved_message",
    )