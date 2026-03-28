-- ==============================
-- Internal Parquet Table
-- ==============================
-- Description:
-- This internal table stores the dataset in Parquet format
-- under Hive-managed storage.

CREATE TABLE nyctaxi_internal_parquet (
    VendorID INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    RatecodeID BIGINT,
    store_and_fwd_flag STRING,
    PULocationID INT,
    DOLocationID INT,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE
)
STORED AS PARQUET;

-- Load data from external Parquet table
-- Ensures consistent dataset for performance comparison
INSERT OVERWRITE TABLE nyctaxi_internal_parquet
SELECT * FROM nyctaxi_external_parquet;

-- ==============================
-- Internal Textfile Table
-- ==============================
-- Description:
-- This internal table stores the dataset in Textfile format
-- for comparison against Parquet storage.

CREATE TABLE nyctaxi_internal_textfile (
    VendorID INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    RatecodeID BIGINT,
    store_and_fwd_flag STRING,
    PULocationID INT,
    DOLocationID INT,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE
)
STORED AS TEXTFILE;

-- Load same dataset from external Parquet table
-- Ensures fair comparison across table types and formats
INSERT OVERWRITE TABLE nyctaxi_internal_textfile
SELECT * FROM nyctaxi_external_parquet;
