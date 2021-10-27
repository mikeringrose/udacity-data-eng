from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    r"""
    Copies data from the staging tables to a fact table.

    :param redshift_conn_id: the ID of the Redshift connection
    :param aws_credentials_id: the ID of the AWS credentials to be used
    :param table: the table to populate
    :param query: a SQL to extract the data from the staging tables
    """
    ui_color = '#F98866'

    insert_sql_template = """
        INSERT INTO {}
        {};
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
    
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = LoadFactOperator.insert_sql_template.format(
            self.table,
            self.query
        )
        self.log.info(f"Loading data into the {table} fact table...")
        redshift.run(formatted_sql)
