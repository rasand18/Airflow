from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
import re

# Lista över tabeller med parametrar för varje
TABLES = [
    {"table_name": "/m3/MVXCJDB.CSYTAB"},
    {"table_name": "/m3/MVXCJDB.OOLINE"},
    {"table_name": "/m3/MVXCJDB.OOHEAD"},
    {"table_name": "/m3/MVXCJDB.ODHEAD"},
    {"table_name": "/m3/MVXCJDB.MITMAS"},
    {"table_name": "/m3/MVXCJDB.MITHRY"},
    {"table_name": "/m3/MVXCJDB.MITPOP"},
    {"table_name": "/m3/MVXCJDB.MITISH"},
    {"table_name": "/m3/MVXCJDB.OLICHA"},
    {"table_name": "/m3/MVXCJDB.MITISH"}
    # {"table_name": "/m3/MVXCJDB.CSYTAB", "queue_name": "medium", "driver_cores": 2, "driver_memory": "2G", "executor_instances": 3, "executor_cores": 2, "executor_memory": "2G"},
]

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 10, 31),
    'catchup': False
}

# Funktion för att konvertera tabellnamn till Kubernetes-kompatibla namn
def normalize_k8s_name(name):
    return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")

# Skapa DAG
with DAG(
    dag_id="kafka-to-base",
    default_args=default_args,
    schedule=timedelta(days=1),
    template_searchpath='/opt/airflow/dags/repo/dags/',
    catchup=False
) as dag:

    for config in TABLES:
        normalized_table_name = normalize_k8s_name(config["table_name"])

        # Spark Kubernetes Operator-task
        spark_task = SparkKubernetesOperator(
            task_id=f"spark_task_{normalized_table_name}",
            namespace="spark-operator",
            application_file="sparkTransformToAzure.yaml",  # Din Spark YAML-template
            kubernetes_conn_id="spark-k8s",
            do_xcom_push=False,
            params={  # Skicka bara det som behövs
                "spark_app_name": normalized_table_name,
                "table_name": config.get("table_name"),
                "queue_name": config.get("queue_name"),
                "driver_cores": config.get("driver_cores"),
                "driver_memory": config.get("driver_memory"),
                "executor_instances": config.get("executor_instances"),
                "executor_cores": config.get("executor_cores"),
                "executor_memory": config.get("executor_memory"),
            },
        )

        # Sensor för att övervaka Spark-jobbet
        sensor_task = SparkKubernetesSensor(
            task_id=f"monitor_{normalized_table_name}",
            namespace="spark-operator",
            application_name=f"spark-app-{normalized_table_name}",  # Matchar application_name i din YAML
            kubernetes_conn_id="spark-k8s",
            attach_log=True
        )

        # Definiera task-beroenden
        spark_task >> sensor_task
