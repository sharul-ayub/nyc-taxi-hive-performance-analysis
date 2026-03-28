-- ==============================
-- Bucketed Textfile Table
-- ==============================
-- Description:
-- This table uses bucketing based on PULocationID
-- to organize data into fixed number of buckets.

-- Why bucketing:
-- Bucketing distributes rows into predefined buckets
-- based on a hash of the column (PULocationID).
-- This can improve performance for joins and aggregations
-- by reducing shuffle and improving data locality.

-- Why PULocationID:
-- It is commonly used in joins (with zone lookup table),
-- making it suitable as bucketing key.

-- Why 8 buckets:
-- Provides balanced data distribution while avoiding too many small files.

CREATE TABLE nyctaxi_bucketed_textfile (
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
CLUSTERED BY (PULocationID) INTO 8 BUCKETS
STORED AS TEXTFILE;

-- Enforce bucketing during data insertion
SET hive.enforce.bucketing=true;

-- Insert data into bucketed table
INSERT OVERWRITE TABLE nyctaxi_bucketed_textfile
SELECT * FROM nyctaxi_internal_parquet;

-- ==============================
-- Bucketed Parquet Table
-- ==============================
-- Description:
-- Same bucketing strategy as Textfile table,
-- but stored in Parquet format for performance comparison.

CREATE TABLE nyctaxi_bucketed_parquet (
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
CLUSTERED BY (PULocationID) INTO 8 BUCKETS
STORED AS PARQUET;

-- Enforce bucketing during data insertion
SET hive.enforce.bucketing=true;

-- Insert same dataset for fair comparison
INSERT OVERWRITE TABLE nyctaxi_bucketed_parquet
SELECT * FROM nyctaxi_internal_parquet;
