from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime
import subprocess

# Default arguments för Airflow-tasks
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

# Funktion för att dynamiskt lista DBT-modeller
def get_dbt_models():
    # Kör dbt ls för att lista alla modeller (fungerar lokalt eller med rätt context i containern)
    result = subprocess.run(
        ["dbt", "ls", "--resource-type", "model"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Error fetching DBT models: {result.stderr}")
    return result.stdout.strip().split("\n")

with DAG('dbt_test_dynamic', default_args=default_args, schedule_interval=None) as dag:

    # Base KubernetesPodOperator-config
    base_operator_args = {
        "namespace": "spark-operator",  # Namespace där podden körs
        "image": "harbor.ad.spendrups.se/dataplatform-test/dbt:1.0",  # DBT-image
        "cmds": ["sh", "-c"],
        "get_logs": True,
        "image_pull_policy": "Always"
    }

    # Första task: Se till att DBT-beroenden hämtas
    dbt_deps = KubernetesPodOperator(
        task_id="dbt_deps",
        arguments=["dbt deps"],
        name="dbt-deps-pod",
        **base_operator_args
    )

    # Dynamiskt skapa tasks för varje DBT-modell
    try:
        dbt_models = get_dbt_models()
    except RuntimeError as e:
        raise ValueError(f"Kunde inte hämta DBT-modeller: {e}")

    previous_task = dbt_deps  # För att skapa task-beroenden
    for model in dbt_models:
        dbt_task = KubernetesPodOperator(
            task_id=f"dbt_run_{model}",
            arguments=[f"dbt run --select {model}"],
            name=f"dbt-run-{model}-pod",
            **base_operator_args
        )
        previous_task >> dbt_task  # Lägg till beroende mellan tasks
        previous_task = dbt_task
