import os

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}';
    """
    s3_prefix = 's3://'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 table_name,
                 s3_bucket,
                 s3_key,
                 json_option='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_option = json_option

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Clearing data from {self.table_name} staging table in Redshift')
        redshift.run("DELETE FROM {}".format(self.table_name))

        rendered_s3_key = self.s3_key.format(**context)
        s3_path = os.path.join(StageToRedshiftOperator.s3_prefix, self.s3_bucket, rendered_s3_key)
        self.log.info(f'Copying data from S3 to {self.table_name} staging table in Redshift')
        self.log.info(f'Data source: {s3_path}')
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_option
        )

        redshift.run(formatted_sql)




