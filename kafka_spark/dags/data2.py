from airflow import DAG
import datetime
import json
from airflow.operators.python_operator import PythonOperator
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

def producer_function():
    for i in range(20):
        yield (json.dumps(i), json.dumps(i + 1))

default_args={
    'start_date':datetime.datetime(2022,1,1)
}

with DAG('first',default_args=default_args,schedule_interval='@once',catchup=False) as dags:
    t1 = ProduceToTopicOperator(
        task_id='t1',
        topic="test_1",
        producer_function="data2.producer_function",
        kafka_config={"bootstrap.servers": "broker:29092"},
    )

    t1