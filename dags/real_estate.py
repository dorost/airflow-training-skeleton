import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable,Connection
from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from datetime import datetime
from dateutil.parser import parse
from airflow_training.operators.httpgcs import HttpToGcsOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator


dag = DAG(
    dag_id="real_estate",
    default_args={
        "owner": "godatadriven",
        "start_date":  airflow.utils.dates.days_ago(30),
    },
)
prints_started_job = BashOperator(
    task_id="prints_started_job", bash_command="echo {{ execution_date }}", dag=dag
)


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id='read_from_pgs',
    sql= "select * from land_registry_price_paid_uk where transfer_date = '{{ ds }}'",
    bucket='amin-bucket2',
    filename='data/{{ ds }}/real_estate.json',
    dag=dag
)
currency_jobs = []
for currency in ['EUR', 'USD']:
        currency_jobs.append(
            HttpToGcsOperator(
            task_id= 'get_rates_' + currency,
            endpoint= '/convert-currency?date={{ ds }}&from=GBP&to=' + currency,
            gcs_path = 'currency/{{ ds }}/' + currency + '.json',
            gcs_bucket= 'amin-bucket2',
            dag=dag
        )
    )



load_into_bigquery = DataFlowPythonOperator(
    task_id="land_registry_prices_to_bigquery",
    dataflow_default_options={
        'region': "europe-west1",
        'input': 'gs://amin-bucket2/*/*.json',
        'temp_location': 'gs://amin-bucket2/temp/',
        'staging_location': 'gs://amin-bucket2/staging/',
        'table': 'amin',
        'dataset': 'amin',
        'project': 'airflowbolcom-a2262958ad7773ed',
        'project_id': 'airflowbolcom-a2262958ad7773ed',
        'bucket': 'amin-bucket2',
        'name': '{{ task_instance_key_str }}'
    },
    py_file="gs://amin-bucket2/dataflow_job.py",
    dag=dag,
)


