-- ==============================
-- Partitioned + Bucketed Textfile Table
-- ==============================
-- Description:
-- This table combines both partitioning and bucketing strategies.

-- Partitioning (pickup_datetime):
-- - Organizes data by date in HDFS
-- - Enables partition pruning (scan only relevant dates)
-- - Reduces I/O for date-filtered queries

-- Bucketing (PULocationID):
-- - Distributes data into 8 buckets using hash function
-- - Improves performance for joins on PULocationID
-- - Helps reduce shuffle during join operations

CREATE TABLE nyctaxi_partitioned_bucketed_textfile (
    VendorID INT,
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
PARTITIONED BY (pickup_datetime DATE)
CLUSTERED BY (PULocationID) INTO 8 BUCKETS
STORED AS TEXTFILE;

-- Enable bucketing and dynamic partitioning
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Insert data with partition + bucket structure
INSERT INTO TABLE nyctaxi_partitioned_bucketed_textfile PARTITION (pickup_datetime)
SELECT 
    VendorID,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee,
    cbd_congestion_fee,
    to_date(pickup_datetime) AS pickup_datetime
FROM nyctaxi_internal_parquet;

-- ==============================
-- Partitioned + Bucketed Parquet Table
-- ==============================
-- Description:
-- Same combined strategy as Textfile table,
-- but stored in Parquet format for optimal performance.

CREATE TABLE nyctaxi_partitioned_bucketed_parquet (
    VendorID INT,
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
PARTITIONED BY (pickup_datetime DATE)
CLUSTERED BY (PULocationID) INTO 8 BUCKETS
STORED AS PARQUET;

-- Enable bucketing and dynamic partitioning
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Insert standardized dataset
INSERT INTO TABLE nyctaxi_partitioned_bucketed_parquet PARTITION (pickup_datetime)
SELECT 
    VendorID,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee,
    cbd_congestion_fee,
    to_date(pickup_datetime) AS pickup_datetime
FROM nyctaxi_internal_parquet;
