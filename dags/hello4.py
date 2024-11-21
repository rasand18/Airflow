from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('dbt_test', default_args=default_args, schedule_interval=None) as dag:

    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='cd /opt/airflow/dags/repo/dags/dbt/dbt_dataplatform_test && dbt debug'
    )
    