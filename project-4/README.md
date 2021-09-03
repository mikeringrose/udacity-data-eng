# Project 4: Data Lake

In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## Getting Started

This project requires Python 3 and to get started following the steps below:

1. Copy `dl.example.cfg` to `dl.cfg` and replace the keys `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` with you AWS credentials
2. Install the project requirements running `pip install -r requirements.txt`
3. Run `python etl.py`

This will create the folder `./tables` which will contain the "normalized" database tables.

## Sparkify Database

In order to handle the increasing demands of the analytics and ML teams, Sparkify wants to move their JSON log files on song plays to a "data lake." To support these users a the JSON log files
are being processing into a new DB modeled after a star schema. The database consists of one fact table, songplays, and four dimensions -- users, artists, songs, and time. Star schema was chosen
to provide simple joins and simple queries to support various analytics requests. The image below depicts the tables and their relationships.

![Sparkify DB](sparkify-schema.png)

## ETL Pipeline

