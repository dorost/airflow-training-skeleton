import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from bq import BigQueryGetDataOperator
from airflow.operators.slack_operator import SlackAPIOperator
from airflow.operators.python_operator import PythonOperator
import random
from slack import MySlackAPIOperator
from airflow.models import Variable

dag = DAG(
    dag_id="hello_airflow",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)

prints_started_job = BashOperator(
    task_id="prints_started_job", bash_command="echo {{ execution_date }}", dag=dag
)


QUERY = """
SELECT
  name
FROM (
  SELECT
    author.name AS name,
    COUNT(*) AS count
  FROM
    `bigquery-public-data.github_repos.commits`
  WHERE
    "apache/airflow" IN UNNEST(repo_name)
  GROUP BY
    author.name
  ORDER BY
    count DESC
  LIMIT
    5 )
"""

get_data = BigQueryGetDataOperator(
    task_id='get_data_from_bq',
    sql=QUERY,
    provide_context=True,
    dag=dag
)


publish_to_slack = MySlackAPIOperator(
    token=Variable.get("token"),
    task_id = "publish_it",
    provide_context=True,
    dag=dag
)

get_data >> publish_to_slack