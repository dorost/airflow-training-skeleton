import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
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


options = ['job_one', 'job_two']

def pick_a_random_branch():
    return random.choice(options)


branching = BranchPythonOperator(
    task_id="prints_logs_job", python_callable=pick_a_random_branch, dag=dag
)

final_task = BashOperator(
    task_id="final_task", bash_command="echo 'done'", trigger_rule= TriggerRule.ONE_SUCCESS, dag=dag
)

for option in options: 
    branching >> DummyOperator(task_id= option , dag=dag) >> final_task
