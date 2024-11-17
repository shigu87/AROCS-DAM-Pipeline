from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'DAM_ARCOS_batch_spark_job',
    default_args=default_args,
    description='A DAG to run Spark job on Dataproc for DAM-ARCOS processing',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 18),
    catchup=False,
    tags=['dev'],
    params={
        'execution_date': Param(default='NA', type='string', description='Execution date in yyyymmdd format'),
        'can_id': Param(default='NA', type='string', description='CAN_ID to process'),
    }
)

# Fetch configuration from Airflow variables
config = Variable.get("cluster_details", deserialize_json=True)
CLUSTER_NAME = config['CLUSTER_NAME']
PROJECT_ID = config['PROJECT_ID']
REGION = config['REGION']
ZONE = config['ZONE']

# Paths for PySpark job and parameters
pyspark_job_file_path = 'gs://your-bucket/spark_code/DAM_ARCOS_processing.py'

# Python function to get the execution date and CAN_ID
def get_execution_date_and_canid(ds_nodash, **kwargs):
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    can_id = kwargs['params'].get('can_id', 'NA')
    return execution_date, can_id

# Create Dataproc cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    zone=ZONE,
    num_workers=2,
    dag=dag,
)

# PythonOperator to fetch the execution date and CAN_ID
get_execution_date_and_canid_task = PythonOperator(
    task_id='get_execution_date_and_canid',
    python_callable=get_execution_date_and_canid,
    provide_context=True,
    op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    dag=dag,
)

# Submit the PySpark job
submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_job',
    main=pyspark_job_file_path,
    arguments=[
        '--date={{ ti.xcom_pull(task_ids="get_execution_date_and_canid")[0] }}',  # Passing date from the task
        '--can_id={{ ti.xcom_pull(task_ids="get_execution_date_and_canid")[1] }}',  # Passing can_id from the task
    ],
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Delete Dataproc cluster
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    dag=dag,
)

# Set task dependencies
create_cluster >> get_execution_date_and_canid_task >> submit_pyspark_job >> delete_cluster

