
import json

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.slack_hook import SlackHook
from airflow.exceptions import AirflowException

from airflow.operators.slack_operator import SlackAPIOperator

class MySlackAPIOperator(SlackAPIOperator):

    @apply_defaults
    def __init__(self,
                 slack_conn_id=None,
                 token=None,
                 method=None,
                 api_params=None,
                 *args, **kwargs):
        super(SlackAPIOperator, self).__init__(*args, **kwargs)

        if token is None and slack_conn_id is None:
            raise AirflowException('No valid Slack token nor slack_conn_id supplied.')
        if token is not None and slack_conn_id is not None:
            raise AirflowException('Cannot determine Slack credential '
                                   'when both token and slack_conn_id are supplied.')

        self.token = token
        self.slack_conn_id = slack_conn_id

        self.method = method
        self.api_params = api_params
    def execute(self, context):

        data = context['task_instance'].xcom_pull(task_ids='get_data_from_bq')
        self.log.info('Context is ' + str(data))
        aaa = ','.join( [str(v[0]) for v in data])
        if not self.api_params:
            self.construct_api_call_params()

        self.api_params = {}
        self.api_params['text'] = 'Amin says: ' + 'Daniel please change your user name its too long'
        self.api_params['icon_url'] = "https://cdn3.iconfinder.com/data/icons/essentials-pack-part-1/128/Essentials_Pack-96-512.png"
        self.api_params['username'] = "Amin Dorostanian"
        self.api_params['channel'] = 'general'
        slack = SlackHook(token=self.token, slack_conn_id=self.slack_conn_id)
        self.method = 'chat.postMessage'
        slack.call(self.method, self.api_params)
