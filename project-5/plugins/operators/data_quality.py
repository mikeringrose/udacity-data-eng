from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

operator_lookup = {
    'gt': lambda result, value : result > value,
    'gte': lambda result, value : result >= value,
    'lt': lambda result, value : result < value,
    'lte': lambda result, value : result <= value,
    'eq': lambda result, value : result == value
}

class DataQualityOperator(BaseOperator):
    r"""
    Checks that the supplied tables contain data.

    :param redshift_conn_id: the ID of the Redshift connection
    :param checks: an array of sql checks to validate the loaded table, each item
                    in the array should be a dictionary with the following keys:
                    "test_sql", "expected", "comparison". comparison should be one
                    of the following: "gt", "gte", "lt", "lte", "eq"
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check in self.checks:
            sql_stmt = check['test_sql']
            operator = check['comparison']
            value = check['value']

            records = redshift_hook.get_records(sql_stmt)

            if len(records) < 1:
                raise ValueError(f"Data quality check failed. \"{sql_stmt}\" returned no results")
        
            result = records[0][0]

            check_fn = operator_lookup[operator]

            if not check_fn(result, value):
                raise ValueError(f"Data quality check failed. \"{sql_stmt}\" failed")

            self.log.info(f"Data quality checks passed")
