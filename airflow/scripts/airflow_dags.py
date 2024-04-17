from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator # type: ignore
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator # type: ignore
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # type: ignore
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='Data Pipeline DAG',
    schedule_interval=timedelta(days=1),
)

# Define the directory paths
BASE_DIR = '/path/to/your/repo'
KAFKA_DIR = os.path.join(BASE_DIR, 'kafka', 'scripts')
PYSPARK_DIR = os.path.join(BASE_DIR, 'pyspark', 'scripts')
POSTGRES_DIR = os.path.join(BASE_DIR, 'postgres', 'scripts')
DBT_DIR = os.path.join(BASE_DIR, 'dbt', 'models')
GDS_DIR = os.path.join(BASE_DIR, 'google_data_studio', 'scripts')
TERRAFORM_DIR = os.path.join(BASE_DIR, 'terraform', 'scripts')

# Terraform tasks
terraform_apply = PythonOperator(
    task_id='terraform_apply',
    python_callable=lambda: os.system(f'cd {TERRAFORM_DIR} && terraform apply'),
    dag=dag,
)

# Kafka tasks
ingestion = PythonOperator(
    task_id='ingestion',
    python_callable=lambda: os.system(f'python {KAFKA_DIR}/ingestion.py'),
    dag=dag,
)

# PySpark tasks
pyspark_transform = PythonOperator(
    task_id='pyspark_transform',
    python_callable=lambda: os.system(f'python {PYSPARK_DIR}/pyspark_transform.py'),
    dag=dag,
)

# Postgres tasks
init_postgres = PythonOperator(
    task_id='init_postgres',
    python_callable=lambda: os.system(f'psql -h postgres -U postgres -f {POSTGRES_DIR}/init.sql'),
    dag=dag,
)

gds_to_postgres = PythonOperator(
    task_id='gds_to_postgres',
    python_callable=lambda: os.system(f'python {POSTGRES_DIR}/gds_to_postgres.py'),
    dag=dag,
)

postgres_transform = PythonOperator(
    task_id='postgres_transform',
    python_callable=lambda: os.system(f'python {POSTGRES_DIR}/postgres_transform.py'),
    dag=dag,
)

postgres_to_bigquery = PythonOperator(
    task_id='postgres_to_bigquery',
    python_callable=lambda: os.system(f'python {POSTGRES_DIR}/postgres_to_bigquery.py'),
    dag=dag,
)

# DBT tasks
dbt_run = PythonOperator(
    task_id='dbt_run',
    python_callable=lambda: os.system(f'dbt run --profiles-dir {DBT_DIR}'),
    dag=dag,
)

# Google Data Studio tasks
authentication = PythonOperator(
    task_id='authentication',
    python_callable=lambda: os.system(f'python {GDS_DIR}/authentication.py'),
    dag=dag,
)

gds_report = PythonOperator(
    task_id='gds_report',
    python_callable=lambda: os.system(f'python {GDS_DIR}/GDS_report.py'),
    dag=dag,
)

data_fetch_transform = PythonOperator(
    task_id='data_fetch_transform',
    python_callable=lambda: os.system(f'python {GDS_DIR}/data_fectch_transform.py'),
    dag=dag,
)

update_report = PythonOperator(
    task_id='update_report',
    python_callable=lambda: os.system(f'python {GDS_DIR}/update_report.py'),
    dag=dag,
)

create_dashboard = PythonOperator(
    task_id='create_dashboard',
    python_callable=lambda: os.system(f'python {GDS_DIR}/create_dashboard.py'),
    dag=dag,
)

# Define task dependencies
terraform_apply >> ingestion >> pyspark_transform >> init_postgres >> gds_to_postgres >> postgres_transform >> postgres_to_bigquery >> dbt_run >> authentication >> gds_report >> data_fetch_transform >> update_report >> create_dashboard
