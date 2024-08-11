create database clickstream_db;

CREATE TABLE prod_schema.events (
    id BIGINT,
    user_id BIGINT,
    session_id CHAR(36),
    ip_address VARCHAR(50),
    city VARCHAR(255),
    state VARCHAR(255),
    browser VARCHAR(50),
    is_mobile VARCHAR(3),
    traffic_source VARCHAR(50),
    traffic_source_category VARCHAR(50),
    uri VARCHAR(255),
    event_type VARCHAR(50)
);