from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
import boto3

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 10, 31),
    'catchup': False
}

# Funktion för att testa MinIO-anslutning
def test_minio_connection():
    # Hämta anslutningsdetaljer från Airflow
    conn = BaseHook.get_connection("minio_conn")
    
    # Hämta credentials och endpoint från 'extra'
    extra_config = conn.extra_dejson
    access_key = extra_config.get("aws_access_key_id")
    secret_key = extra_config.get("aws_secret_access_key")
    endpoint_url = extra_config.get("endpoint_url")

    print(access_key)
    print(secret_key)
    print(endpoint_url)
    # Skapa S3-klient för MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Försök att lista buckets som ett anslutningstest
    try:
        response = s3_client.list_buckets()
        print("Connection to MinIO successful. Buckets:", [bucket["Name"] for bucket in response.get("Buckets", [])])
    except Exception as e:
        print(f"Connection test failed: {e}")

dag = DAG(
    'hej',
    default_args=default_args,
    schedule=timedelta(days=1),
    template_searchpath='/opt/airflow/dags/repo/dags/'
)

# Task för att testa anslutningen till MinIO
minio_connection_test_task = PythonOperator(
    task_id='test_minio_connection',
    python_callable=test_minio_connection,
    dag=dag
)

minio_connection_test_task
# # Skapa parallella Spark tasks och sensorer
# num_tasks = 10  # Antalet parallella instanser
# spark_tasks = []
# sensor_tasks = []

# for i in range(num_tasks):
#     spark_task = SparkKubernetesOperator(
#         task_id=f'n-spark-on-k8s-airflow-{i+1}',
#         trigger_rule="all_success",
#         depends_on_past=False,
#         retries=0,
#         application_file='spark-pi.yaml',
#         namespace="spark-operator",
#         kubernetes_conn_id="spark-k8s",
#         do_xcom_push=True,
#         dag=dag
#     )

#     sensor_task = SparkKubernetesSensor(
#         task_id=f'spark_pi_monitor_{i+1}',
#         namespace="spark-operator",
#         application_name="{{ task_instance.xcom_pull(task_ids='n-spark-on-k8s-airflow-" + str(i + 1) + "')['metadata']['name'] }}",
#         kubernetes_conn_id="spark-k8s",
#         dag=dag,
#         attach_log=True
#     )

#     cancel_task = KubernetesPodOperator(
#             task_id=f"cancel_spark_job_{i+1}",
#             namespace="default",
#             name=f"cancel-spark-job-{i+1}",
#             image="bitnami/kubectl:latest",
#             cmds=["kubectl", "delete", "sparkapplication", f"spark-pi-n-spark-on-k8s-airflow-{i+1}"],
#             dag=dag,
#             trigger_rule="all_done"  # Körs endast om den manuellt triggas
#     )

#     spark_task >> sensor_task
#     sensor_task >> cancel_task  # Definiera beroendet mellan task och sensor
#     spark_tasks.append(spark_task)
#     sensor_tasks.append(sensor_task)

# # Kör alla Spark tasks parallellt
# spark_tasks