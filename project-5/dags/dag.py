from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from subdag import get_load_dimension_dag
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

"""
This DAG populates the Sparkify Data warehouse performing the following tasks:
* Staging the data from S3
* Loading the songplays fact table
* Performing data quality checks on the fact table
* Loading the supporting dimension tables and performing data quality check on each via a subdag
The dag starts loading data at 2018-01-11. If the data fails it will retry 3 times with a 5 minutes delay
"""

start_date = datetime(2018, 1, 11)

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False
}

dag_id = "project_5_dag"

dag = DAG(dag_id,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_path="s3://udacity-dend/log_json_path.json",
    # s3_key='log_data/{{ execution_date.strftime("%Y/%m") }}',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A/A',   
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table='songplays',
    query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = SubDagOperator(
    subdag=get_load_dimension_dag(
        dag_id,
        "Load_user_dim_table",
        "redshift",
        "users",
        SqlQueries.user_table_insert,
        start_date=start_date
    ),
    task_id="Load_user_dim_table",
    dag=dag,
)

load_song_dimension_table = SubDagOperator(
    subdag=get_load_dimension_dag(
        dag_id,
        "Load_song_dim_table",
        "redshift",
        "songs",
        SqlQueries.song_table_insert,
        start_date=start_date
    ),
    task_id="Load_song_dim_table",
    dag=dag,
)

load_artist_dimension_table = SubDagOperator(
    subdag=get_load_dimension_dag(
        dag_id,
        "Load_artist_dim_table",
        "redshift",
        "artists",
        SqlQueries.artist_table_insert,
        start_date=start_date
    ),
    task_id="Load_artist_dim_table",
    dag=dag,
)

load_time_dimension_table = SubDagOperator(
    subdag=get_load_dimension_dag(
        dag_id,
        "Load_time_dim_table",
        "redshift",
        "time",
        SqlQueries.time_table_insert,
        start_date=start_date
    ),
    task_id="Load_time_dim_table",
    dag=dag,
)

run_songplays_quality_checks = DataQualityOperator(
    task_id='Run_songplays_quality_checks',
    redshift_conn_id="redshift",
    tables=["songplays"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> run_songplays_quality_checks
run_songplays_quality_checks >> load_user_dimension_table
run_songplays_quality_checks >> load_artist_dimension_table
run_songplays_quality_checks >> load_song_dimension_table
run_songplays_quality_checks >> load_time_dimension_table
load_user_dimension_table >> end_operator
load_artist_dimension_table >> end_operator
load_song_dimension_table >> end_operator
load_time_dimension_table >> end_operator
