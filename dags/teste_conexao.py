from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta


def simple_test():
    hook = S3Hook(aws_conn_id='AWS-Airflow')
    print("Connection established:", hook.conn_id)
    buckets = hook.list_buckets()
    for bucket in buckets:
        print(f"Bucket Name: {bucket.name}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'test_s3_connection',
    default_args=default_args,
    description='Test S3 connection',
    schedule_interval=None,
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id='test_s3_connection',
        python_callable=simple_test
    )
