from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BigQueryGetDataOperator(BaseOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and returns data in a python list. The number of elements in the returned list will
    be equal to the number of rows fetched. Each element in the list will again be a list
    where element would represent the columns values for that row.
    **Example Result**: ``[['Tony', '10'], ['Mike', '20'], ['Steve', '15']]``
    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'``.
    **Example**: ::
        get_data = BigQueryGetDataOperator(
            task_id='get_data_from_bq',
            sql='SELECT * FROM [bigquery-public-data:github_repos.commits]',
            bigquery_conn_id='airflow-service-account'
        )
    :param sql: The BigQuery SQL to execute.
    :type sql: str
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    """
    template_fields = ('sql',)
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(self,
                 sql,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 max_rows=1925,
                 *args,
                 **kwargs):
        super(BigQueryGetDataOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.max_rows = max_rows

    def execute(self, context):
        self.log.info('Fetching Data from:')
        self.log.info('Query: %s', self.sql)

        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                            delegate_to=self.delegate_to)

        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        response = cursor.fetchmany(self.max_rows)

        self.log.info('Total Extracted rows: %s', len(response))

        self.log.info('Response: %s', response)
        return response