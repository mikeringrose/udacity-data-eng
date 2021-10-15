import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender CHAR,
    itemInSession INT,
    lastName TEXT,
    length NUMERIC,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration DOUBLE PRECISION,
    sessionId INT,
    song TEXT,
    status INT,
    ts timestamp,
    userAgent TEXT,
    userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR(18),
    num_songs INT,
    title TEXT,
    artist_name TEXT,
    artist_latitude NUMERIC,
    year INT,
    duration NUMERIC,
    artist_id TEXT,
    artist_longitude NUMERIC,
    artist_location TEXT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INT NOT NULL REFERENCES users(user_id),
    level TEXT,
    song_id TEXT REFERENCES songs(song_id),
    artist_id TEXT REFERENCES artists(artist_id),
    session_id INT,
    location TEXT,
    user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT PRIMARY KEY,
    title TEXT,
    artist_id TEXT,
    year NUMERIC,
    duration NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    artist_name TEXT,
    artist_location TEXT,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} 
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT json {}
TIMEFORMAT as 'epochmillisecs'
BLANKSASNULL EMPTYASNULL TRUNCATECOLUMNS;
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN').strip("'"),
    config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
COPY staging_songs FROM {} 
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT json 'auto'
BLANKSASNULL EMPTYASNULL TRUNCATECOLUMNS;
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN').strip("'")
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT DISTINCT 
    e.ts,
    e.userid,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionid,
    s.artist_location,
    e.useragent
FROM 
    staging_songs s
        JOIN staging_events e
            ON s.title = e.song
                AND s.artist_name = e.artist
                AND s.duration = e.length;
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    userid,
    firstname,
    lastname,
    gender,
    level
FROM staging_events
WHERE userid IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
SELECT DISTINCT 
    ts, 
    EXTRACT(HOUR FROM ts),
    EXTRACT(DAY FROM ts),
    EXTRACT(WEEK FROM ts),
    EXTRACT(MONTH FROM ts),
    EXTRACT(YEAR FROM ts),
    EXTRACT(WEEKDAY FROM ts)
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
