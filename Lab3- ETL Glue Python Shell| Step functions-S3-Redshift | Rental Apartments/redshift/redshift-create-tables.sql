create database db_rental_apartments;

create schema raw_zone;

CREATE TABLE raw_zone.apartments (
  id BIGINT PRIMARY KEY,
  title VARCHAR(255),
  source VARCHAR(50),
  price DECIMAL(10, 2),
  currency VARCHAR(10),
  listing_created_on TIMESTAMP,
  is_active BOOLEAN,
  last_modified_timestamp TIMESTAMP
);

CREATE TABLE raw_zone.apartment_attributes (
  id BIGINT NOT NULL,
  category VARCHAR(255),
  body varchar(2000),
  amenities TEXT,
  bathrooms DECIMAL(3,1) DEFAULT NULL,
  bedrooms DECIMAL(3,1) DEFAULT NULL,
  fee DECIMAL(10,2) DEFAULT NULL,
  has_photo VARCHAR(10) DEFAULT NULL,
  pets_allowed VARCHAR(255) DEFAULT NULL,
  price_display VARCHAR(255) DEFAULT NULL,
  price_type VARCHAR(50) DEFAULT NULL,
  square_feet INT DEFAULT NULL,
  address VARCHAR(255) DEFAULT NULL,
  cityname VARCHAR(100) DEFAULT NULL,
  state VARCHAR(50) DEFAULT NULL,
  latitude DECIMAL(10,7) DEFAULT NULL,
  longitude DECIMAL(10,7) DEFAULT NULL
);

CREATE TABLE raw_zone.apartment_viewings (
  user_id INT,
  apartment_id BIGINT,
  viewed_at TIMESTAMP,
  is_wishlisted CHAR(1),
  call_to_action VARCHAR(50)
);

create table raw_zone.tmp_apartments as select * from raw_zone.apartments;

create table raw_zone.tmp_apartment_attributes as select * from raw_zone.apartment_attributes;

create table raw_zone.tmp_apartment_viewings as select * from raw_zone.apartment_viewings;

create schema processed_zone;

CREATE TABLE processed_zone.fact_apartment_viewings (
    viewing_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    apartment_id BIGINT NOT NULL ,
    user_id INT NOT NULL,
    viewed_at TIMESTAMP,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(50),
    price DECIMAL(10, 2),
    fee DECIMAL(10, 2),
    currency VARCHAR(10)
);

CREATE TABLE processed_zone.dim_apartments (
    apartment_id BIGINT PRIMARY KEY,
    title VARCHAR(255),
    category VARCHAR(255),
    body VARCHAR(2000),
    amenities TEXT,
    bedrooms DECIMAL(3,1),
    bathrooms DECIMAL(3,1),
    square_feet INT,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(50),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    has_photo VARCHAR(10),
    pets_allowed VARCHAR(255),
    price_display VARCHAR(255),
    price_type VARCHAR(50)
);

CREATE TABLE processed_zone.dim_users (
    user_id INT PRIMARY KEY
);
