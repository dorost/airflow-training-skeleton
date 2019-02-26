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

dag = DAG(
    dag_id="real_estate_job",
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

http_gcs = HttpToGcsOperator(
    task_id= 'get_rates',
    endpoint= '/convert-currency?date={{ ds }}&from=GBP&to=EUR',
    gcs_path = 'currency/{{ ds }}/rates.json',
    gcs_bucket= 'amin-bucket2',
    dag=dag
)


prints_started_job >> pgsl_to_gcs >> http_gcs

