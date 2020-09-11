from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tests,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            record = redshift.get_records(test['sql'])
            if record is None or len(record) == 0 or len(record[0]) == 0:
                raise ValueError(f"Data quality check failed. No result was returned.")

            if record[0][0] != test['result']:
                raise ValueError(f"Data quality check failed. Got {record[0][0]} when expecting {test['result']}.")

        self.log.info(f"Data quality checks cleared.")