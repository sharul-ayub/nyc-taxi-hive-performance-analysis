-- ==============================
-- Partitioned Textfile Table
-- ==============================
-- Description:
-- This table is partitioned by pickup_datetime (DATE)
-- to improve query performance for date-based filtering.

-- Why partitioning:
-- Partitioning physically organizes data by date in HDFS,
-- allowing Hive to scan only relevant partitions instead of full table.
-- This reduces I/O and improves execution time (partition pruning).

-- Why DATE type:
-- pickup_datetime is converted to DATE to match partition column
-- and support efficient filtering (e.g., WHERE date BETWEEN ...).

-- Dynamic partitioning:
-- Enabled to automatically create partitions during data insertion.

CREATE TABLE nyctaxi_partitioned_textfile (
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
STORED AS TEXTFILE;

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Insert data with DATE conversion for partition column
INSERT INTO TABLE nyctaxi_partitioned_textfile PARTITION (pickup_datetime)
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
-- Partitioned Parquet Table
-- ==============================
-- Description:
-- Same partitioning strategy as Textfile table, but stored in Parquet format
-- to evaluate combined impact of partitioning + columnar storage.

CREATE TABLE nyctaxi_partitioned_parquet (
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
STORED AS PARQUET;

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Insert data with DATE conversion for partition column
INSERT INTO TABLE nyctaxi_partitioned_parquet PARTITION (pickup_datetime)
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
