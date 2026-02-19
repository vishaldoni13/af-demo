from pendulum import datetime
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

def my_callable_task(**context):
    # Access the param via the context dictionary
    print(f"Received value: {context['params']['my_int_param']}")

with DAG(
    dag_id="a_example_param_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None, # Typically None for param-triggered DAGs
    catchup=False,
    params={
        "my_param": "default_value", # Simple string default
        "my_int_param": Param(23, type="integer", minimum=1), # Param with validation
    },
) as dag:
    PythonOperator(
        task_id="print_param_task",
        python_callable=my_callable_task,
    )
