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
    bash_command='[ -f /opt/airflow/dags/repo/values.yaml ] && cat /opt/airflow/dags/repo/dags/values.yaml || echo "File does not exist"',
    dag=dag
)

t1
