import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.slack_operator import SlackAPIOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable,Connection
from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

dag = DAG(
    dag_id="real_estate_job",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)
prints_started_job = BashOperator(
    task_id="prints_started_job", bash_command="echo {{ execution_date }}", dag=dag
)


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id='read_from_pgs',
    sql= 'select * from land_registry_price_paid_uk',
    bucket='gs://amin-bucket2',
    filename='test.txt',
    dag=dag
)

prints_started_job >> pgsl_to_gcs