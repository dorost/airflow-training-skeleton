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
    sql=QUERY
)

pull_data = PythonOperator(
    task_id= 'pull_data',
    python_callable = get_data,
    provide_context=True,
    dag=dag,
)

publish_to_slack = MySlackAPIOperator(
    token="xoxp-559854890739-559228586160-561116849751-2c717700dd7b7a197765ac21770c9c08"
)

pull_data >> publish_to_slack