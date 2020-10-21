import configparser


# DROP TABLES

animes_table_drop = "DROP TABLE IF EXISTS animes"
relations_table_drop = "DROP TABLE IF EXISTS anime_relations"
producers_table_drop = "DROP TABLE IF EXISTS producers"
licensors_table_drop = "DROP TABLE IF EXISTS licensors"
studios_table_drop = "DROP TABLE IF EXISTS studios"
genres_table_drop = "DROP TABLE IF EXISTS genres"
staging_anime_list_table_drop = "DROP TABLE IF EXISTS staging_anime_list"
users_table_drop = "DROP TABLE IF EXISTS users"
staging_favorite_table_drop = "DROP TABLE IF EXISTS staging_favorite"
anime_list_table_drop = "DROP TABLE IF EXISTS anime_list"
# CREATE TABLES

animes_table_create= ("""
    CREATE TABLE IF NOT EXISTS animes (
        animeID BIGINT NOT NULL SORTKEY,
        title VARCHAR(500),
        title_english VARCHAR(500),
        type VARCHAR,
        source VARCHAR,
        episodes DECIMAL,
        status VARCHAR,
        duration VARCHAR,
        rating VARCHAR,
        score FLOAT4,
        scored_by DECIMAL,
        members INTEGER,
        favorites INTEGER,
        aired_from DATE,
        aired_to DATE,
        broadcast VARCHAR,
        premiered VARCHAR
    )
""")

relations_table_create= ("""
    CREATE TABLE IF NOT EXISTS anime_relations (
        animeID BIGINT NOT NULL SORTKEY DISTKEY,
        relation VARCHAR,
        id_relation BIGINT,
        relation_type VARCHAR
    )
""")


producers_table_create= ("""
    CREATE TABLE IF NOT EXISTS producers (
        animeID BIGINT NOT NULL SORTKEY DISTKEY,
        producer_id INTEGER,
        producer_name VARCHAR
    )
""")

licensors_table_create= ("""
    CREATE TABLE IF NOT EXISTS licensors (
        animeID BIGINT NOT NULL SORTKEY DISTKEY,
        licensor_id INTEGER,
        licensor_name VARCHAR
    )
""")

studios_table_create= ("""
    CREATE TABLE IF NOT EXISTS studios (
        animeID BIGINT NOT NULL SORTKEY DISTKEY,
        studio_id INTEGER,
        studio_name VARCHAR
    )
""")

genres_table_create= ("""
    CREATE TABLE IF NOT EXISTS genres (
        animeID BIGINT NOT NULL SORTKEY DISTKEY,
        genre_id INTEGER,
        genre_name VARCHAR
    )
""")

staging_anime_list_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_anime_list (
        username VARCHAR NOT NULL DISTKEY,
        animeID BIGINT NOT NULL SORTKEY,
        watching_status INTEGER NOT NULL,
        score INTEGER,
        watched_episodes INTEGER,
        is_rewatching BOOLEAN NOT NULL,
        watch_start_date DATE NULL,
        watch_end_date DATE NULL,
        days BIGINT,
        priority VARCHAR(20)        
    )
""")

users_table_create= ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT NOT NULL,
        username VARCHAR NOT NULL SORTKEY,
        last_online TIMESTAMP,
        gender VARCHAR,
        birthday DATE,
        location VARCHAR,
        joined DATE
    )
""")

staging_favorites_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_favorite (
        username VARCHAR NOT NULL DISTKEY,
        animeID BIGINT SORTKEY
    )
""")

anime_list_table_create= ("""
    CREATE TABLE IF NOT EXISTS anime_list (
        username VARCHAR NOT NULL DISTKEY,
        animeID BIGINT NOT NULL SORTKEY,
        watching_status INTEGER NOT NULL,
        score INTEGER,
        watched_episodes INTEGER,
        is_rewatching BOOLEAN NOT NULL,
        watch_start_date DATE NULL,
        watch_end_date DATE NULL,
        days BIGINT,
        priority VARCHAR(20),
        favorite BOOLEAN NOT NULL
    )
""")

# FINAL TABLES
animes_list_table_insert = ("""
    INSERT INTO anime_list (
        username,
        animeID,
        watching_status,
        score,
        watched_episodes,
        is_rewatching,
        watch_start_date,
        watch_end_date,
        days,
        priority,
        favorite
    )
    SELECT  DISTINCT 
    
        al.username,
        al.animeID,
        al.watching_status,
        al.score,
        al.watched_episodes,
        al.is_rewatching,
        al.watch_start_date,
        al.watch_end_date,
        al.days,
        al.priority,
        CASE
            WHEN f.animeID IS NULL THEN FALSE 
            ELSE TRUE 
        END AS favorite
        
    FROM staging_anime_list as al
    left join staging_favorite as f ON al.username = f.username and al.animeID = f.animeID
    ;
""")



# QUERY LISTS

create_table_queries = [animes_table_create, relations_table_create, producers_table_create, licensors_table_create, studios_table_create, genres_table_create, staging_anime_list_table_create, users_table_create, staging_favorites_table_create, anime_list_table_create]

#create_table_queries = [staging_user_table_create, staging_favorites_table_create]

drop_table_queries = [animes_table_drop, relations_table_drop, producers_table_drop, licensors_table_drop, studios_table_drop, genres_table_drop, staging_anime_list_table_drop, users_table_drop, staging_favorite_table_drop, anime_list_table_drop]

#drop_table_queries = [staging_user_table_drop, staging_favorite_table_drop]

insert_table_queries = [animes_list_table_insert]