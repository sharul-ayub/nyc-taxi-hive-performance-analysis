-- ==============================
-- External Parquet Table
-- ==============================
-- Description:
-- This table stores the standardized external dataset in Parquet format.
-- Timestamp fields are converted from microsecond-based BIGINT values
-- into Hive-compatible TIMESTAMP format during insertion.

-- Why this conversion is needed:
-- The original dataset stores tpep_pickup_datetime and
-- tpep_dropoff_datetime in microsecond precision.
-- The Hive version used in this project does not support
-- microsecond-level timestamps directly, so the values are
-- converted using from_unixtime(...) after dividing by 1,000,000.

-- IMPORTANT:
-- Update the LOCATION path, {hdfs_dataset_path} based on your HDFS directory
-- Example: 
-- LOCATION '/user/student5/project_assignment/Dataset';

CREATE EXTERNAL TABLE nyctaxi_external_parquet (
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
STORED AS PARQUET
LOCATION '{hdfs_dataset_path}';

-- Insert data from raw external table
-- Convert BIGINT microsecond timestamps into Hive TIMESTAMP
INSERT INTO nyctaxi_external_parquet
SELECT
    VendorID,
    from_unixtime(CAST(tpep_pickup_datetime / 1000000 AS BIGINT)) AS pickup_datetime,
    from_unixtime(CAST(tpep_dropoff_datetime / 1000000 AS BIGINT)) AS dropoff_datetime,
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
    cbd_congestion_fee
FROM nyctaxi_external_rawdataset;

-- ==========================================================================================================

-- ==============================
-- External Textfile Table
-- ==============================
-- Description:
-- This table stores the same standardized dataset in Textfile format
-- for performance comparison against the Parquet version.
-- The data is copied directly from the external Parquet table
-- so both tables contain the same schema and records.

-- IMPORTANT:
-- Update the LOCATION path, {hdfs_dataset_path} based on your HDFS directory
-- Example: 
-- LOCATION '/user/student5/project_assignment/Dataset';

CREATE EXTERNAL TABLE nyctaxi_external_textfile (
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
STORED AS TEXTFILE
LOCATION '{hdfs_dataset_path}';

-- Copy standardized data from external Parquet table
INSERT OVERWRITE TABLE nyctaxi_external_textfile
SELECT * FROM nyctaxi_external_parquet;
