from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG definition
with DAG(
    dag_id="test_dag",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Simple test DAG",
    schedule_interval=None,   # chỉ chạy khi bấm trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test"],
) as dag:

    task1 = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    task2 = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello Airflow!'"
    )

    task1 >> task2
