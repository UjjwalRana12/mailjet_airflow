from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'intellypod',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='intellypod_mailjet_dag',
    default_args=default_args,
    description='Run main.py then mail.py for Intellypod Mailjet workflow',
    schedule_interval=None, 
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['intellypod', 'mailjet'],
) as dag:

    run_main = BashOperator(
        task_id='run_main_py',
        bash_command='python "{{ dag_run.conf.get(\'main_path\', \'main.py\') }}"',
        cwd='{{ dag_run.conf.get("cwd", "/home/airflow/gcs/data") }}'
    )

    run_mail = BashOperator(
        task_id='run_mail_py',
        bash_command='python "{{ dag_run.conf.get(\'mail_path\', \'mail.py\') }}"',
        cwd='{{ dag_run.conf.get("cwd", "/home/airflow/gcs/data") }}'
    )

    run_main >> run_mail