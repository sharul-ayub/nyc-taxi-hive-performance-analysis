-- ==============================
-- Raw External Table (NYC Taxi Dataset)
-- ==============================

-- Description:
-- Timestamp fields (tpep_pickup_datetime, tpep_dropoff_datetime)
-- are stored as BIGINT because the original data uses microsecond precision.
-- The Hive version used does not support microsecond-level timestamps,
-- so values are stored as BIGINT and will be converted to TIMESTAMP later
-- to ensure compatibility.

-- IMPORTANT:
-- Update the LOCATION path, {hdfs_dataset_path} based on your HDFS directory
-- Example: 
-- LOCATION '/user/student5/project_assignment/Dataset';


CREATE EXTERNAL TABLE nyctaxi_external_rawdataset (
    VendorID INT,
    tpep_pickup_datetime BIGINT,
    tpep_dropoff_datetime BIGINT,
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
STORED AS PARQUET
LOCATION '{hdfs_dataset_path}';
