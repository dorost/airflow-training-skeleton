import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from bq import BigQueryGetDataOperator
from airflow.operators.slack_operator import SlackAPIOperator

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



publish_to_slack = SlackAPIOperator(
        token="xoxp-559854890739-559228586160-560304790661-ae28d681f2f1026dd05cfc0a42f27d89",
        task_id='publish',
        text='hello',
        channel='General',  # Replace with your Slack username
        username='airflow'
)