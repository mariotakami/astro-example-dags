from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Função para copiar o arquivo do S3 e executar o script Python
def execute_python_from_s3():
    hook = S3Hook(aws_conn_id='AWS-Airflow')  # Use o ID da conexão configurada no Airflow
    source_key = 'notebooks/airflow/job_mensalidade.py'
    source_bucket = 'uniodonto-in10'
    local_path = '/tmp/job_mensalidade.py'
    
    # Baixa o arquivo do S3 para o caminho local
    hook.download_file(key=source_key, bucket_name=source_bucket, local_path=local_path)

    # Executa o script Python
    import subprocess
    subprocess.run(['python', local_path], check=True)

# Definição dos argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuração da DAG
with DAG(
    'mensalidade_uni',
    default_args=default_args,
    description='DAG para executar um script Python a partir do S3',
    schedule_interval=None,  # None para execução manual
    catchup=False,
) as dag:

    # Task 1: Executar o script Python do S3
    execute_python_script = PythonOperator(
        task_id='execute_python_script',
        python_callable=execute_python_from_s3
    )

    # Task 2: Mensagem de erro (executada se Task 1 falhar)
    error_message = BashOperator(
        task_id='error_message',
        bash_command='echo "A execução da DAG falhou."',
        trigger_rule='one_failed'
    )

    # Task 3: Mensagem de sucesso (executada se Task 1 for bem-sucedida)
    success_message = BashOperator(
        task_id='success_message',
        bash_command='echo "A execução da DAG foi bem-sucedida."',
        trigger_rule='all_success'
    )

    # Definindo a ordem de execução das tarefas
    execute_python_script >> [success_message, error_message]
