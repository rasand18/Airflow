from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 10, 20),
    'catchup': False
}

dag = DAG(
    'hello_world',
    default_args = default_args,
    schedule=timedelta(days=1)
)

t1 = BashOperator(
    task_id = 'hello_world',
    bash_command='sleep 300 && echo "Hello World"',
    queue='kubernetes',
    dag = dag
)

t2 = BashOperator(
    task_id = 'hello_dml',
    bash_command='sleep 300 && echo "Hello World"',
    dag = dag
)

t1 >> t2