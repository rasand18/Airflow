from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 10, 20),
    'catchup': False
}

dag = DAG(
    'check_directory',
    default_args=default_args,
    schedule=timedelta(days=1)
)

# Kontrollera om katalogen finns
t1 = BashOperator(
    task_id='check_directory_exists',
    bash_command='[ -d /opt/airflow/dags/repo/dags ] && echo "Directory exists" || echo "Directory does not exist"',
    queue='kubernetes',
    dag=dag
)

t1
