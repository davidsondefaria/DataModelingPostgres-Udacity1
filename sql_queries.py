# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id SERIAL PRIMARY KEY, 
                            start_time time UNIQUE, 
                            user_id varchar NOT NULL,
                            level varchar,
                            song_id varchar NOT NULL,
                            artist_id varchar NOT NULL,
                            session_id varchar NOT NULL,
                            location varchar,
                            user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
                            user_id varchar PRIMARY KEY,
                            first_name varchar NOT NULL,
                            last_name varchar,
                            gender varchar,
                            level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
                            song_id varchar PRIMARY KEY,
                            title varchar NOT NULL,
                            artist_id varchar NOT NULL,
                            year int,
                            duration numeric
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
                            artist_id varchar PRIMARY KEY,
                            artist_name varchar NOT NULL,
                            artist_location varchar,
                            artist_latitude numeric,
                            artist_longitude numeric
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
                            start_time time PRIMARY KEY,
                            hour int,
                            day int,
                            week int,
                            month int,
                            year int ,
                            weekday int
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
				songplay_id, 
				start_time, 
				user_id,
				level,
				song_id,
				artist_id,
				session_id,
				location,
				user_agent
			)
VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time)
DO UPDATE SET user_id = EXCLUDED.user_id,
              level = EXCLUDED.level,
			  artist_id = EXCLUDED.artist_id,
			  session_id = EXCLUDED.session_id,
			  location = EXCLUDED.location,
			  user_agent = EXCLUDED.user_agent
""")

user_table_insert = ("""
INSERT INTO users (
				user_id,
				first_name,
				last_name,
				gender,
				level
			)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO UPDATE SET first_name = EXCLUDED.first_name,
              last_name = EXCLUDED.last_name,
			  gender = EXCLUDED.gender,
			  level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs (
				song_id,
				title,
				artist_id,
				year,
				duration
			)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO UPDATE SET title = EXCLUDED.title,
			  artist_id = EXCLUDED.artist_id,
			  year = EXCLUDED.year,
              duration = EXCLUDED.duration
""")

artist_table_insert = ("""
INSERT INTO artists (
				artist_id,
				artist_name,
				artist_location,
				artist_latitude,
				artist_longitude
			)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO UPDATE SET artist_name = EXCLUDED.artist_name,
			  artist_location = EXCLUDED.artist_location,
			  artist_latitude = EXCLUDED.artist_latitude,
			  artist_longitude = EXCLUDED.artist_longitude
""")


time_table_insert = ("""
INSERT INTO time (
				start_time,
				hour,
				day,
				week,
				month,
				year,
				weekday
			)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time)
DO UPDATE SET hour = EXCLUDED.hour,
			  day = EXCLUDED.day,
			  week = EXCLUDED.week,
			  month = EXCLUDED.month,
			  year = EXCLUDED.year,
			  weekday = EXCLUDED.weekday
""")

# FIND SONGS

song_select = ("""
SELECT songs.song_id, artists.artist_id
FROM (songs JOIN artists ON songs.artist_id=artists.artist_id)
WHERE songs.title = %s
AND artists.artist_name = %s
AND songs.duration = %s
""")

# """
# SELECT song_id, artist_id
# FROM song
# WHERE songs.title = %s
# AND artists.artist_name = %s
# AND songs.duration = %s
# """

#time.start_time, users.user_id, users.level, song_id, artist_id, session_id, location, user_agent

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]