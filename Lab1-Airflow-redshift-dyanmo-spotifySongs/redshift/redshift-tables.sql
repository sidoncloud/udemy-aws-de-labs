create database songs_db;

create schema reporting_schema;

CREATE TABLE reporting_schema.genre_level_kpis (
    listen_date DATE NOT NULL,
    track_genre VARCHAR(255) NOT NULL,
    listen_count INT,
    popularity_index FLOAT,
    average_duration FLOAT,
    most_popular_track_id VARCHAR(255)
);

CREATE TABLE reporting_schema.tmp_genre_level_kpis (
    listen_date DATE NOT NULL,
    track_genre VARCHAR(255) NOT NULL,
    listen_count INT,
    popularity_index FLOAT,
    average_duration FLOAT,
    most_popular_track_id VARCHAR(255)
);

CREATE TABLE reporting_schema.hourly_kpis (
    listen_date DATE NOT NULL,
    listen_hour INT NOT NULL,
    unique_listeners INT,
    listen_counts INT,
    top_artist VARCHAR(255),
    avg_sessions_per_user FLOAT,
    diversity_index FLOAT,
    most_engaged_age_group VARCHAR(255)
);

CREATE TABLE reporting_schema.tmp_hourly_kpis (
    listen_date DATE NOT NULL,
    listen_hour INT NOT NULL,
    unique_listeners INT,
    listen_counts INT,
    top_artist VARCHAR(255),
    avg_sessions_per_user FLOAT,
    diversity_index FLOAT,
    most_engaged_age_group VARCHAR(255)
);