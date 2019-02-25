import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

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



prints_logs_job = BashOperator(
    task_id="prints_logs_job", bash_command="echo 'I did the job now'", dag=dag
)

prints_started_job >> prints_logs_job