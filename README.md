
## Project Goal

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We will be building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. We will be able to test our database and ETL pipeline by running queries given to us by the analytics team from Sparkify and compare our results with their expected results.


## Datasets

## Songs Dataset
{"num_songs": 1, "artist_id": "AR051VM1187B9B7F27", "artist_latitude": 52.478589999999997, "artist_longitude": -1.9086000000000001, "artist_location": "Birmingham, England", "artist_name": "Chicken Shack", "song_id": "SOYAGAF12A8C13E21A", "title": "Andalucian Blues", "duration": 138, "year": 2005}


## Events Dataset
{"artist":"Radiohead", "auth":"Logged In", "firstName":"Ayleen", "gender", "F", "itemInSession":0, "lastName":"Wise", "length":130.82077000000001, "level":"free", "location":"Columbia, SC", "method":"PUT", "page":"NextSong", "registration":"1541085793796", "sessionId":70, "song":"Pop Is Dead", "status":200, "ts":1541183813796, "userAgent":"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D201 Safari/9537.53", "userId":71}


## Databases

| Table | Description |
| ---- | ---- |
| staging_events | stating table for event dataset |
| staging_songs | staging table for songs datset |
| songplays | information how songs were played, e.g. when by which user in which session | 
| users | user-related information such as name, gender and level | 
| songs | song-related information containing name, artist, year and duration | 
| artists | artist name and location (geo-coords and textual location) | 
| time | time-related info for timestamps | 


## Project instructions
1. Setup the redshift cluster on AWS as instructed
2. Edit the connection details in `dwh.cfg`.
3. Drop/Create the database tables by executing `create_tables.py` from terminal or python console
4. Start the data pipeline first loading the stageing area and then loading the tables from that stanig area by running `etl.py` from terminal or python console


## Example queries
* Find all users from a specific location: ```SELECT DISTINCT users.user_id FROM users JOIN songplays ON songplays.user_id = users.user_id WHERE songplays.location = <LOCATION>```
* Find all songs by a specific artist: ```SELECT songs.song_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id WHERE artist.name = <ARTIST>```