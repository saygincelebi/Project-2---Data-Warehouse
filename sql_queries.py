import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# Redshift default diststyle=AUTO


staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                        artist      VARCHAR,
                                        auth        VARCHAR,
                                        firstName   VARCHAR,
                                        gender      VARCHAR,
                                        itemInSession VARCHAR,
                                        lastName    VARCHAR,
                                        length      VARCHAR,
                                        level       VARCHAR,
                                        location    VARCHAR,
                                        method      VARCHAR,
                                        page        VARCHAR,
                                        registration VARCHAR,
                                        sessionId   INTEGER NOT NULL SORTKEY DISTKEY,
                                        song        VARCHAR,
                                        status      INTEGER,
                                        ts          BIGINT NOT NULL,
                                        userAgent   VARCHAR,
                                        userId      INTEGER
                                 )""")



staging_songs_table_create =  ("""CREATE  TABLE IF NOT EXISTS staging_songs(
                                            num_songs           INTEGER,
                                            artist_id           VARCHAR NOT NULL SORTKEY DISTKEY,
                                            artist_latitude     VARCHAR,
                                            artist_longitude    VARCHAR,
                                            artist_location     VARCHAR (500),
                                            artist_name         VARCHAR (500),
                                            song_id             VARCHAR NOT NULL,
                                            title               VARCHAR (500),
                                            duration            DECIMAL (9),
                                            year                INTEGER
                                            )
                                    """)



songplay_table_create =  ("""CREATE TABLE IF NOT EXISTS songplays(
                                    songplay_id INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
                                    start_time  TIMESTAMP               NOT NULL,
                                    user_id     VARCHAR(50)             NOT NULL DISTKEY,
                                    level       VARCHAR(20)             NOT NULL,
                                    song_id     VARCHAR(50)             NOT NULL,
                                    artist_id   VARCHAR(50)             NOT NULL,
                                    session_id  VARCHAR(50)             NOT NULL,
                                    location    VARCHAR(100)            NULL,
                                    user_agent  VARCHAR(255)            NULL)""")



user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                                user_id     INTEGER         NOT NULL SORTKEY,
                                first_name  VARCHAR(50)     NULL,
                                last_name   VARCHAR(50)     NULL,
                                gender      VARCHAR(1)     NULL,
                                level       VARCHAR(10)     NULL) diststyle all;""")




song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                                song_id     VARCHAR(60)     NOT NULL SORTKEY,
                                title       VARCHAR(500)    NOT NULL,
                                artist_id   VARCHAR(50)     NOT NULL,
                                year        INTEGER         NOT NULL,
                                duration    DECIMAL(9)      NOT NULL )""")




artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                            artist_id   VARCHAR(60)             NOT NULL SORTKEY,
                            name        VARCHAR(500)            NULL,
                            location    VARCHAR(500)            NULL,
                            latitude    DECIMAL(10)              NULL,
                            longitude   DECIMAL(10)              NULL)diststyle all;""")






time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                                start_time  TIMESTAMP   NOT NULL SORTKEY,
                                hour        SMALLINT    NULL,
                                day         SMALLINT    NULL,
                                week        SMALLINT    NULL,
                                month       SMALLINT    NULL,
                                year        SMALLINT    NULL,
                                weekday     SMALLINT    NULL
                            ) diststyle all;""")




# STAGING TABLES

staging_events_copy = (f"""copy staging_events 
                          from {LOG_DATA}
                          iam_role {IAM_ROLE}
                          json {LOG_JSONPATH}
                          STATUPDATE ON
                          region 'us-west-2'; """)


staging_songs_copy = (f"""copy staging_songs 
                          from {SONG_DATA} 
                          iam_role {IAM_ROLE}
                          json 'auto'
                          ACCEPTINVCHARS AS '^'
                          STATUPDATE ON
                          region 'us-west-2'
                          ; """)




# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT  timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, se.userId, se.level, 
                                    ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
                            FROM staging_events se, staging_songs ss
                            WHERE se.page = 'NextSong' AND
                            se.song =ss.title AND
                            se.artist = ss.artist_name AND
                            se.length = ss.duration""")


user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT distinct  userId, firstName, lastName, gender, level
                        FROM staging_events se
                        WHERE page = 'NextSong'""")



song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        SELECT song_id, title, artist_id, year, duration
                        FROM staging_songs ss
                        WHERE song_id IS NOT NULL""")



artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                          SELECT distinct artist_id, artist_name, artist_location , artist_latitude, artist_longitude 
                          FROM staging_songs ss
                          WHERE artist_id IS NOT NULL""")




time_table_insert = ("""INSERT INTO time (start_time,
                                                        hour,
                                                        day,
                                                        week,
                                                        month,
                                                        year,
                                                        weekday)
                    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                                * INTERVAL '1 second'        AS start_time,
                            EXTRACT(hour FROM start_time)    AS hour,
                            EXTRACT(day FROM start_time)     AS day,
                            EXTRACT(week FROM start_time)    AS week,
                            EXTRACT(month FROM start_time)   AS month,
                            EXTRACT(year FROM start_time)    AS year,
                            EXTRACT(week FROM start_time)    AS weekday
                    FROM    staging_events                   AS se
                    WHERE se.page = 'NextSong';""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

