from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta

def simple_test():
    # Criação do hook utilizando o ID da conexão configurado no Airflow
    hook = S3Hook(aws_conn_id='AWS-Airflow')
    print("Conexão com S3 estabelecida.")
    
    # Listando os buckets disponíveis na conexão S3
    buckets = hook.list_buckets()
    if buckets:
        print("Buckets encontrados:")
        for bucket in buckets:
            print(f"Bucket Name: {bucket['Name']}")
    else:
        print("Nenhum bucket encontrado.")

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
