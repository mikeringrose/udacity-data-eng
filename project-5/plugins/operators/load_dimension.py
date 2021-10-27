from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql_template = """
        BEGIN;
        INSERT INTO {}
        {};
        COMMIT;
    """
    
    truncate_insert_sql_template = """
        BEGIN;
        TRUNCATE TABLE {};
        INSERT INTO {}
        {};
        COMMIT;
    """    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.append = append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append:
            formatted_sql = LoadDimensionOperator.insert_sql_template.format(
                self.table,
                self.query
            )
        else:
            formatted_sql = LoadDimensionOperator.truncate_insert_sql_template.format(
                self.table,
                self.table,
                self.query
            )
        redshift.run(formatted_sql)
