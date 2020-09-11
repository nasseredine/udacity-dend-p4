from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    delete_sql = 'DELETE FROM {};'
    insert_sql = 'INSERT INTO {} ({});'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 select_sql,
                 append_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.select_sql = select_sql
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_data:
            formatted_delete_sql = LoadDimensionOperator.delete_sql.format(self.table_name)
            redshift.run(formatted_delete_sql)
        formatted_insert_sql = LoadDimensionOperator.insert_sql.format(self.table_name, self.select_sql)
        redshift.run(formatted_insert_sql)
