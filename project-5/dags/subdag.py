import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

"""
A subdag that populates a single dimension table and then performs data quality checks on that table
"""
def get_load_dimension_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        sql_stmt,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    check_rows = {
        "test_sql": f"SELECT COUNT(*) FROM {table}",
        "comparison": "gt",
        "value": 0
    }

    load_dimension_table = LoadDimensionOperator(
        task_id=f"Load_{table}_dim_table",
        redshift_conn_id=redshift_conn_id,
        table=table,
        query=sql_stmt,
        dag=dag
    )

    run_quality_checks = DataQualityOperator(
        task_id=f"Run_{table}_data_quality_checks",
        redshift_conn_id=redshift_conn_id,
        checks=[check_rows],
        dag=dag
    )    

    load_dimension_table >> run_quality_checks

    return dag
