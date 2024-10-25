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
    schedule=timedelta(days=1)
)


spark_application_yaml = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: spark-operator
spec:
  type: Scala
  mode: cluster
  image: spark:3.5.3
  imagePullPolicy: IfNotPresent
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
  arguments:
  - "5000"
  sparkVersion: 3.5.3
  driver:
    labels:
      version: 3.5.3
    cores: 1
    memory: 512m
    serviceAccount: spark-operator-spark
  executor:
    labels:
      version: 3.5.3
    instances: 1
    cores: 1
    memory: 512m
"""

# Kontrollera om katalogen finns
spark_k8s_task = SparkKubernetesOperator(
    task_id='n-spark-on-k8s-airflow',
    trigger_rule="all_success",
    depends_on_past=False,
    retries=0,
    application_file=spark_application_yaml,
    namespace="spark-operator",
    kubernetes_conn_id="spark-k8s",
    do_xcom_push=True,
    dag=dag
)

spark_k8s_task
