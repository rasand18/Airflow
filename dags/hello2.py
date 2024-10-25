from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import boto3
import csv
from io import BytesIO

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': datetime(2024, 10, 25),
    'catchup': False
}

# Funktion för att skapa och ladda upp CSV direkt till MinIO
def create_and_upload_to_minio(bucket_name, object_name):
    # Hämta anslutningsdetaljer från Airflow
    conn = BaseHook.get_connection("minio_conn")
    
    # Hämta credentials och endpoint från 'extra'
    extra_config = conn.extra_dejson
    access_key = extra_config.get("aws_access_key_id")
    secret_key = extra_config.get("aws_secret_access_key")
    endpoint_url = extra_config.get("endpoint_url")

    # Skapa S3-klient för MinIO
    s3_client = boto3.client(
        'aws',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Skapa CSV-innehållet direkt i minnet
    csv_buffer = BytesIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["Name", "Age", "City"])
    writer.writerow(["Alice", 30, "New York"])
    writer.writerow(["Bob", 25, "Los Angeles"])
    writer.writerow(["Charlie", 35, "Chicago"])
    
    # Flytta pekaren till början av buffer för att ladda upp korrekt
    csv_buffer.seek(0)
    
    # Ladda upp filen till MinIO
    try:
        s3_client.upload_fileobj(csv_buffer, bucket_name, object_name)
        print(f"File successfully uploaded to {bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error occurred: {e}")

dag = DAG(
    'hej',
    default_args=default_args,
    schedule=timedelta(days=1),
    template_searchpath='/opt/airflow/dags/repo/dags/'
)

# Task för att skapa och ladda upp CSV till MinIO
minio_upload_task = PythonOperator(
    task_id='create_and_upload_to_minio',
    python_callable=create_and_upload_to_minio,
    op_kwargs={
        'bucket_name': 'my-bucket',          # Ange din bucket-namn
        'object_name': 'uploaded_example_data.csv'  # Namn på filen i bucketen
    },
    dag=dag
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

# Definiera task-beroende
spark_k8s_task >> minio_upload_task
