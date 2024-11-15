from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 10, 31),
    'catchup': False
}

# Skapa en enkel DAG
with DAG(
    'simple_spark_dag',
    default_args=default_args,
    schedule=timedelta(days=1),
    template_searchpath='/opt/airflow/dags/repo/dags/',
    catchup=False
) as dag:

    # Spark Kubernetes Operator för att köra Spark-jobbet
    spark_task = SparkKubernetesOperator(
        task_id='spark_on_k8s',
        application_file='spark.yaml',  # Din Spark-applikationens specifikation
        namespace="spark-operator",
        kubernetes_conn_id="spark-k8s",
        do_xcom_push=True,
        name="spark-python-app-k8s"
    )

    # Sensor för att övervaka Spark-jobbet
    sensor_task = SparkKubernetesSensor(
        task_id='spark_pi_monitor',
        namespace="spark-operator",
        application_name="{{ task_instance.xcom_pull(task_ids='spark_on_k8s')['metadata']['name'] }}",
        kubernetes_conn_id="spark-k8s",
        attach_log=True
    )

    # Definiera task-beroenden
    spark_task >> sensor_task
