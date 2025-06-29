"""
Helper Module: sql_queries.py

This module contains SQL statements used for inserting data into the Sparkify data warehouse.
Each statement is used by a custom Airflow operator to load data from staging tables
into the appropriate fact or dimension table.
"""


class SqlQueries:
    songplay_table_insert = ("""
        SELECT
            md5(events.sessionId || events.start_time) AS playid,
            events.start_time,
            events.userId,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.sessionId,
            events.location,
            events.userAgent
        FROM (
            SELECT
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                *
            FROM staging_events
            WHERE page = 'NextSong'
        ) events
        LEFT JOIN staging_songs songs
            ON events.song = songs.title
           AND events.artist = songs.artist_name
           AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT DISTINCT
            userId,
            firstName,
            lastName,
            gender,
            level
        FROM staging_events
        WHERE page = 'NextSong'
          AND userId IS NOT NULL
    """)

    song_table_insert = ("""
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT DISTINCT
            start_time,
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(dow FROM start_time)
        FROM songplays
    """)
