--create table
CREATE EXTERNAL TABLE IF NOT EXISTS hotel_bookings_processed (
    hotel STRING,
    is_canceled BOOLEAN,
    lead_time INT,
    arrival_date TIMESTAMP,
    stays_in_weekend_nights INT,
    stays_in_week_nights INT,
    adults INT,
    children INT,
    babies INT,
    meal STRING,
    country STRING,
    market_segment STRING,
    distribution_channel STRING,
    is_repeated_guest BOOLEAN,
    previous_cancellations INT,
    previous_bookings_not_canceled INT,
    booking_changes INT,
    deposit_type STRING,
    customer_type STRING,
    adr DOUBLE,
    required_car_parking_spaces INT,
    total_of_special_requests INT,
    reservation_status STRING,
    reservation_status_date TIMESTAMP,
    underage_guests BOOLEAN
)
STORED AS PARQUET 
LOCATION 's3://hotel-data-lake44/processed/';