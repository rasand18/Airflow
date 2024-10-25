from datetime import timedelta 
from datetime import datetime 
import os
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 10, 20),
    'catchup': False
}

dag = DAG(
    'check_directory',
    default_args=default_args,
    schedule=timedelta(days=1),
    template_searchpath='/opt/airflow/dags/repo/dags/'
)

# Kontrollera om katalogen finns
spark_k8s_task = SparkKubernetesOperator(
    task_id='n-spark-on-k8s-airflow',
    trigger_rule="all_success",
    depends_on_past=False,
    retries=0,
    application_file='spark-pi.yaml',
    namespace="spark-operator",
    kubernetes_conn_id="spark-k8s",
    do_xcom_push=True,
    dag=dag
)

spark_k8s_task
