from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 10, 31),
    'catchup': False
}

# Skapa en enkel DAG
with DAG(
    'simple_python_k8s_dag',
    default_args=default_args,
    schedule=timedelta(days=1),
    catchup=False
) as dag:

    # Kör Python-skript i en Kubernetes Pod
    python_k8s_task = KubernetesPodOperator(
        task_id='run_python_script_in_k8s',
        namespace='spark-operator',
        image='harbor.ad.spendrups.se/dataplatform-test/spark-kafka-to-base:1.1',
        cmds=["python", "/app/sparkKafkaToBase.py"],  # Kör din Python-applikation
        name='simple-python-pod',
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True
    )

    # Kör Kubernetes-uppgiften
    python_k8s_task
