from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('dbt_test', default_args=default_args, schedule_interval=None) as dag:

    dbt_debug = KubernetesPodOperator(
        task_id='dbt_debug',
        namespace='spark-operator',  # Namespace där podden körs
        image='harbor.ad.spendrups.se/dataplatform-test/dbt:1.0',
        cmds=["sh", "-c"],
        arguments=["dbt deps && dbt run"],  # Kör både dbt deps och dbt run
        name="dbt-debug-pod",
        get_logs=True,  # Hämta loggar från podden
        image_pull_policy='Always'  # Sätt pulling policy till Always
    )