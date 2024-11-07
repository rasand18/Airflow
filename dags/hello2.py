from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import boto3

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 10, 31),
    'catchup': False
}

dag = DAG(
    'hej',
    default_args=default_args,
    schedule=timedelta(days=1),
    template_searchpath='/opt/airflow/dags/repo/dags/'
)

# Skapa parallella Spark tasks och sensorer
num_tasks = 10  # Antalet parallella instanser

with TaskGroup("spark_tasks_group", dag=dag) as spark_tasks_group:
    spark_tasks = []
    sensor_tasks = []
    
    for i in range(num_tasks):
        spark_task = SparkKubernetesOperator(
            task_id=f'n-spark-on-k8s-airflow-{i+1}',
            trigger_rule="all_success",
            depends_on_past=False,
            retries=0,
            application_file='spark-pi.yaml',
            namespace="spark-operator",
            kubernetes_conn_id="spark-k8s",
            do_xcom_push=True,
            params={"task_instance": f'n-spark-on-k8s-airflow-{i+1}'},  # Pass the task_id
            dag=dag
        )

        sensor_task = SparkKubernetesSensor(
            task_id=f'spark_pi_monitor_{i+1}',
            namespace="spark-operator",
            application_name=f"spark-pi-n-spark-on-k8s-airflow-{i+1}",
            kubernetes_conn_id="spark-k8s",
            dag=dag,
            attach_log=True
        )

        cancel_task = KubernetesPodOperator(
            task_id=f"cancel_spark_job_{i+1}",
            namespace="default",
            name=f"cancel-spark-job-{i+1}",
            image="bitnami/kubectl:latest",
            cmds=["kubectl", "delete", "sparkapplication", f"spark-pi-n-spark-on-k8s-airflow-{i+1}"],
            dag=dag,
            trigger_rule="all_done"  # Körs endast om den manuellt triggas
        )

        # Definiera beroenden
        spark_task >> sensor_task  # Spark-jobb följs av sensor
        sensor_task >> cancel_task  # Lägg cancel som sista steg i TaskGroup

        spark_tasks.append(spark_task)
        sensor_tasks.append(sensor_task)
