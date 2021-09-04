import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.0.3") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(input_data)

    # extract columns to create songs table
    songs_table = df.select(
        df.song_id,
        df.title,
        df.artist_id,
        df.year,
        df.duration
    ).orderBy(df.song_id)

    # write songs table to parquet files partitioned by year and artist
    songs_table_path = f"{output_data}songs_table.parquet"
    songs_table = songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(songs_table_path)

    # extract columns to create artists table
    artists_table = df.select(
        df.artist_id,
        df.artist_name.alias("name"),
        df.artist_location.alias("location"),
        df.artist_latitude.alias("latitude"),
        df.artist_longitude.alias("longitude")
    ).orderBy(df.artist_id).distinct()
    
    # write artists table to parquet files
    artists_table_path = f"{output_data}artists_table.parquet"    
    artists_table = artists_table.write.mode("overwrite").parquet(artists_table_path)

def process_log_data(spark, input_data_logs, input_data_songs, output_data):
    # get filepath to log data file
    log_data = input_data_logs
    songds_data = input_data_songs

    # read log data file
    df = spark.read.json(input_data)

    # filter by actions for song plays
    df_filtered = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df_filtered.select(
        df_filtered.userId.alias("user_id"),
        df_filtered.firstName.alias("first_name"),
        df_filtered.lastName.alias("last_name"),
        df_filtered.level,
        df_filtered.gender
    ).orderBy(df.user_id).distinct()
    
    # write users table to parquet files
    users_table_path = f"{output_data}users_table.parquet"
    users_table.write.mode("overwrite").parquet(users_table_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda t: datetime.fromtimestamp(t / 1000.0), TimestampType())
    df_filtered = df_filtered.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda t: datetime.fromtimestamp(t / 1000.0).isoformat(), StringType())
    df_filtered = df_filtered.withColumn("datetime", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df_filtered.selectExpr([
        "timestamp as start_time",
        "hour(timestamp) as hour",
        "dayofmonth(timestamp) as day",
        "weekofyear(timestamp) as week",
        "month(timestamp) as month",
        "year(timestamp) as year",
        "dayofweek(timestamp) as weekday"
    ]).orderBy("start_time")
    
    # write time table to parquet files partitioned by year and month
    time_table_path = f"{output_data}time_table.parquet"
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(time_table_path)

    # read in song data to use for songplays table
    song_df = spark.read.json(songds_data)

    # extract columns from joined song and log datasets to create songplays table 
    songs_logs_df = df_filtered.join(
        song_df, (df_filtered.artist == song_df.artist_name) & (df_filtered.song == song_df.title)
    )
    songplays_table = songs_logs_df.select(
        monotonically_increasing_id().alias("songplay_id"),
        songs_logs_df.timestamp.alias("start_time"),
        songs_logs_df.userId.alias("user_id"),
        songs_logs_df.level,
        songs_logs_df.artist_id,
        songs_logs_df.sessionId.alias("session_id"),
        songs_logs_df.location,
        songs_logs_df.userAgent.alias("user_agent")
    ).withColumn("year", year("start_time")).withColumn("month", month("start_time"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = f"{output_data}songplays_table.parquet"
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(songplays_table_path)

def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    input_data_songs = "data/song_data/*/*/*/*.json"
    input_data_logs = "data/log-data/*.json"
    output_data = "data/tables/"
    
    process_song_data(spark, input_data_songs, output_data)    
    process_log_data(spark, input_data_logs, input_data_songs, output_data)

if __name__ == "__main__":
    main()
