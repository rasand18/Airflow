from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
import re

# Lista över tabeller
TABLES = ["CSYTAB_clean_system_settings"]

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 10, 31),
    'catchup': False
}

# Funktion för att konvertera tabellnamn till giltiga Kubernetes-namn
def normalize_k8s_name(name):
    return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")

# Skapa DAG
with DAG(
    dag_id="dynamic_spark_table_dag",
    default_args=default_args,
    schedule=timedelta(days=1),
    template_searchpath='/opt/airflow/dags/repo/dags/',
    catchup=False
) as dag:

    for table_name in TABLES:
        normalized_table_name = normalize_k8s_name(table_name)

        # Spark Kubernetes Operator-task
        spark_task = SparkKubernetesOperator(
            task_id=f"spark_task_{normalized_table_name}",
            namespace="spark-operator",
            application_file="sparkTransformToAzure.yaml",  # Din Spark YAML-template
            kubernetes_conn_id="spark-k8s",
            do_xcom_push=False,
            params={"table_name": table_name},  # Skicka dynamiskt tabellnamn till YAML
        )

        # Sensor för att övervaka Spark-jobbet
        sensor_task = SparkKubernetesSensor(
            task_id=f"monitor_{normalized_table_name}",
            namespace="spark-operator",
            application_name=f"spark-python-app-{normalized_table_name}",  # Matchar application_name i din YAML
            kubernetes_conn_id="spark-k8s",
            attach_log=True
        )

        # Definiera task-beroenden
        spark_task >> sensor_task
