from airflow import DAG, Dataset
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import subprocess

# Definir parâmetros básicos da DAG
@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Execução manual
    catchup=False,
    doc_md=__doc__,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=["s3", "scripts"]
)
def mensalidade_uni():
    
    @task
    def execute_python_from_s3():
        hook = S3Hook(aws_conn_id='AWS-Airflow')
        source_key = 'notebooks/airflow/job_mensalidade.py'
        source_bucket = 'uniodonto-in10'
        local_path = '/tmp/job_mensalidade.py'

        # Baixar arquivo do S3
        hook.download_file(key=source_key, bucket_name=source_bucket, local_path=local_path)

        # Executar script Python
        subprocess.run(['python', local_path], check=True)

    @task(trigger_rule='all_success')
    def success_message():
        print("A execução da DAG foi bem-sucedida.")

    @task(trigger_rule='one_failed')
    def error_message():
        print("A execução da DAG falhou.")

    # Definir a ordem de execução das tarefas
    python_task = execute_python_from_s3()
    python_task >> success_message()
    python_task >> error_message()

# Instanciar a DAG
dag_instance = mensalidade_uni()
