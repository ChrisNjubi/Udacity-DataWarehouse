import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Global Variables
IAM_ROLE=config.get("IAM_ROLE","ARN")
LOG_DATA=config.get("S3", "LOG_DATA")
LOG_JSONPATH=config.get("S3","LOG_JSONPATH")
SONG_DATA=config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR(450),
    auth VARCHAR(450),
    firstName VARCHAR(450),
    gender VARCHAR(50),
    itemInSession INTEGER,
    lastName VARCHAR(450),
    length FLOAT,
    level VARCHAR(450),
    location VARCHAR(450),
    method VARCHAR(450),
    page VARCHAR(450),
    registration VARCHAR(450),
    sessionId INTEGER,
    song VARCHAR(65535),
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR(450),
    userId INTEGER
);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs(
    song_id VARCHAR(256),
    artist_id VARCHAR(256),
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(450),
    artist_name VARCHAR(65535),
    duration FLOAT,
    num_songs INTEGER,
    title VARCHAR(65535),
    year INTEGER

);
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id VARCHAR(500),
    level VARCHAR(50),
    song_id VARCHAR(500),
    artist_id VARCHAR(500),
    session_id VARCHAR(500),
    location VARCHAR(500),
    user_agent VARCHAR(500)
);
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users(
    user_id VARCHAR(500) PRIMARY KEY,
    first_name VARCHAR(500),
    last_name VARCHAR(500),
    gender VARCHAR(50),
    level VARCHAR(500)
);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(500) PRIMARY KEY,
    title VARCHAR(500),
    artist_id VARCHAR(500),
    duration FLOAT,
    year INTEGER
    
);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(500) PRIMARY KEY,
    name VARCHAR(500),
    location VARCHAR(500),
    latitude FLOAT,
    longitude FLOAT

);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
);
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events FROM {LOG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ROLE}'
    REGION 'us-east-1'
    FORMAT AS JSON {LOG_JSONPATH}
    TIMEFORMAT AS 'epochmillisecs'
""")

staging_songs_copy = (f"""
    COPY staging_songs FROM {SONG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ROLE}'
    REGION 'us-east-1'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
        se.userid,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionid,
        se.location,
        se.useragent
    FROM staging_events se
    JOIN staging_songs ss ON ss.artist_name = se.artist AND se.page = 'NextSong' AND se.song = ss.title
""")

user_table_insert = ("""
    INSERT INTO users (user_id,first_name,last_name,gender,level)
    SELECT 
        se4.userid,
        se4.firstname,
        se4.lastname,
        se4.gender,
        se4.level
     FROM 
         staging_events se4
     JOIN (
         SELECT 
             userid,
             MAX(ts) AS max_time_stamp
         FROM
             staging_events
         WHERE
             page = 'NextSong'
         GROUP BY
             userid
     ) se5 ON se4.userid = se5.userid AND se4.ts = se5.max_time_stamp
     WHERE 
         se4.page = 'NextSong';
        
""")

song_table_insert = ("""
    INSERT INTO songs (song_id,title,artist_id,duration,year)
    SELECT 
        song_id,
        title,
        artist_id,
        duration,
        year
    FROM 
        staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists( artist_id,name,location,latitude,longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour,day,week,month,year,weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT (hour FROM start_time),
        EXTRACT (day FROM start_time),
        EXTRACT (week FROM start_time),
        EXTRACT (month FROM start_time),
        EXTRACT (year FROM start_time),
        EXTRACT (weekday FROM start_time) 
    FROM staging_events
    WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
