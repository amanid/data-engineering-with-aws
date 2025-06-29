-- ==========================================
-- SQL INSERT STATEMENTS FOR SPARKIFY DWH
-- ==========================================

-- FACT TABLE: songplays
INSERT INTO songplays (
    playid,
    start_time,
    userid,
    level,
    song_id,
    artist_id,
    sessionid,
    location,
    user_agent
)
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
                 TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
             *
         FROM staging_events
         WHERE page = 'NextSong'
     ) events
         LEFT JOIN staging_songs songs
                   ON events.song = songs.title
                       AND events.artist = songs.artist_name
                       AND events.length = songs.duration;


-- DIMENSION TABLE: users
INSERT INTO users (
    userid,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong'
  AND userId IS NOT NULL;


-- DIMENSION TABLE: songs
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs;


-- DIMENSION TABLE: artists
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs;


-- DIMENSION TABLE: time
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT
    start_time,
    EXTRACT(hour FROM start_time),
    EXTRACT(day FROM start_time),
    EXTRACT(week FROM start_time),
    EXTRACT(month FROM start_time),
    EXTRACT(year FROM start_time),
    EXTRACT(dow FROM start_time)
FROM songplays;
