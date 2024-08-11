mysql --local-infile=1 -h {mysql-endpoint} -P 3306 -u admin -p

create database rental_apartments;

use rental_apartments;

CREATE TABLE apartments (
    id BIGINT PRIMARY KEY,
    title VARCHAR(255),
    source VARCHAR(50),
    price DECIMAL(10, 2),
    currency VARCHAR(10),
    listing_created_on DATETIME,
    is_active BOOLEAN,
    last_modified_timestamp DATETIME
);

CREATE TABLE apartment_attributes (
    id BIGINT NOT NULL,
    category VARCHAR(255),
    body TEXT,
    amenities TEXT,
    bathrooms DECIMAL(3,1),
    bedrooms DECIMAL(3,1),
    fee DECIMAL(10,2),
    has_photo VARCHAR(10),
    pets_allowed VARCHAR(255),
    price_display VARCHAR(255),
    price_type VARCHAR(50),
    square_feet INT,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(50),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    PRIMARY KEY (id)
);

CREATE TABLE apartment_viewings (
    user_id INT,
    id BIGINT,
    viewed_at DATETIME,
    is_wishlisted CHAR(1),
    call_to_action VARCHAR(50)
);

LOAD DATA LOCAL INFILE "data/apartments.csv" INTO TABLE rental_apartments.apartments FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE "data/apartment_attributes.csv" INTO TABLE rental_apartments.apartment_attributes FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE "data/user_viewings.csv" INTO TABLE rental_apartments.apartment_viewings FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
