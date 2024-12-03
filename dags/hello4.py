from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from cosmos import DbtDag
from datetime import datetime



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 2),
    'retries': 1
}

# Skapa DAG
with DAG(
    dag_id='dbt_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # Skapa dbt DAG med Cosmos
    dbt_dag = DbtDag(
        dag_id="dbt",  # Unikt ID för DAG
        dbt_project_dir="/dataplatform_spendrups_test",  # Path till ditt dbt-projekt
        dbt_profiles_dir="/dataplatform_spendrups_test",  # Path till dbt-profiler
        operator=KubernetesPodOperator,  # Kör dbt med KubernetesPodOperator
        operator_args={
            "namespace": "spark-operator",  # Namespace där KubernetesPod körs
            "image": "harbor.ad.spendrups.se/dataplatform-test/dbt:1.0",  # Din dbt Docker-image
            "cmds": ["sh", "-c"],  # Kör kommandon i sh
            "arguments": ["dbt deps && dbt run"],  # dbt-kommandon
            "get_logs": True,  # Hämta loggar från podden
            "is_delete_operator_pod": True,  # Rensa pods efter körning
            "image_pull_policy": "Always",  # Dra alltid senaste image
        },
    )