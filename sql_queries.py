import configparser

# LOAD THE CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# THIS SECTION OF CODE REMOVES THE TABLES IF THEY ALREADY EXISTS IN THE
# DATABASE

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# THIS SECTION CONTAINS THE DDL STATEMENTS FOR CREATING THE TABLES
staging_events_table_create = ("""
                             CREATE TABLE IF NOT EXISTS staging_events(artist_name varchar, \
                             auth varchar, \
                             user_first_name varchar, \
                             user_gender  varchar, \
                             item_in_session int, \
                             user_last_name varchar, \
                             song_length decimal, \
                             user_level varchar, \
                             location varchar, \
                             method varchar, \
                             page varchar, \
                             registration varchar, \
                             session_id bigint, \
                             song_title varchar, \
                             status int, \
                             ts bigint, \
                             user_agent text, \
                             user_id varchar);
                             """)

staging_songs_table_create = ("""
                             CREATE TABLE IF NOT EXISTS staging_songs(song_id varchar, \
                             num_songs int, \
                             artist_id varchar, \
                             artist_latitude decimal, \
                             artist_longitude decimal, \
                             artist_location varchar, \
                             artist_name varchar, \
                             title varchar, \
                             duration decimal, \
                             year int);
                             """)
songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplays (songplay_id int identity primary key not null, \
                         start_time timestamp not null, \
                         user_id varchar not null, \
                         level varchar not null , \
                         song_id varchar  null, \
                         artist_id varchar null , \
                         session_id int not null, \
                         location varchar not null, \
                         user_agent varchar not null);
                         """)

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS users (user_id varchar  primary key, \
                     first_name varchar,  \
                     last_name varchar,  \
                     gender varchar, \
                     level varchar);
""")

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS songs (song_id varchar primary key , \
                     title varchar, \
                     artist_id varchar, \
                     year int, \
                     duration decimal);
""")

artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artists (artist_id varchar primary key , \
                       name varchar, \
                       location varchar,\
                       latitude decimal null, \
                       longitude decimal null);
""")

time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time (start_time timestamp  primary key, \
                     hour int, \
                     day int, \
                     week int,\
                     month int, \
                     year int, \
                     weekday int);
""")
# lOAD THE STAGING TABLES FROM JSON UNDER S3

staging_events_copy = ("""copy staging_events
                          from {}
                          iam_role {}
                          json {};
                       """).format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs
                          from {}
                          iam_role {}
                          json 'auto';
                      """).format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# lOAD REMAINING TABLES FROM THE STAGING TABLES LOADED ABOVE

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time,
    e.user_id,
    e.user_level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
FROM staging_events e, staging_songs s
WHERE e.page = 'NextSong'
AND e.song_title = s.title
AND e.artist_name = s.artist_name
AND e.song_length = s.duration
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    user_id,
    user_first_name,
    user_last_name,
    user_gender,
    user_level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
SELECT start_time,
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time),
    extract(month from start_time),
    extract(year from start_time),
    extract(dayofweek from start_time)
FROM songplays
""")


# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert]
