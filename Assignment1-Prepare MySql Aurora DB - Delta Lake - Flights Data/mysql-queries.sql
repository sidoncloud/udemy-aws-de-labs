mysql --local-infile=1 -h flights-database.cluster-cx8wyigc288z.us-east-2.rds.amazonaws.com -P 3306 -u admin -p

create database flights_db;

use flights_db;

CREATE TABLE flight_logistics (
    flight_id VARCHAR(20) PRIMARY KEY,
    fuel_usage DECIMAL(15,8),
    maintenance_check TINYINT,
    baggage_errors TINYINT,
    safety_incidents TINYINT
);

CREATE TABLE passenger_experience (
    flight_id VARCHAR(20),
    passenger_count INT,
    seat_occupancy DECIMAL(3,2),
    customer_satisfaction TINYINT,
    catering_score TINYINT,
    entertainment_score TINYINT,
    cabin_crew_score TINYINT,
    PRIMARY KEY (flight_id),
    FOREIGN KEY (flight_id) REFERENCES flight_logistics(flight_id)
);

CREATE TABLE flights (
    flight_id VARCHAR(20) PRIMARY KEY,
    airline VARCHAR(50),
    flight_number INT,
    aircraft_type VARCHAR(50),
    departure_airport VARCHAR(10),
    arrival_airport VARCHAR(10),
    scheduled_departure_time DATETIME,
    actual_departure_time DATETIME,
    scheduled_arrival_time DATETIME,
    actual_arrival_time DATETIME,
    gate VARCHAR(10),
    terminal VARCHAR(5),
    created_at DATETIME
);

LOAD DATA LOCAL INFILE "flights-data/flight_logistics.csv" INTO TABLE flights_db.flight_logistics FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE "flights-data/passenger_experience.csv" INTO TABLE flights_db.passenger_experience FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE "flights-data/flights.csv" INTO TABLE flights_db.flights FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
