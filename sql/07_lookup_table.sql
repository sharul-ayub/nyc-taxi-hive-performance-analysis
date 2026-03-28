-- ==============================
-- External Lookup Table (Taxi Zone)
-- ==============================
-- Description:
-- This external table stores taxi zone reference data
-- used for joining with the main NYC taxi trip dataset.

-- Purpose:
-- The main dataset only contains location IDs (PULocationID),
-- which are not human-readable.
-- This lookup table maps LocationID → Borough, Zone, and service zone,
-- enabling meaningful analysis (e.g., trips by borough).

-- Join logic:
-- Main table (PULocationID) = Lookup table (LocationID)

-- Data format:
-- Stored as CSV (Textfile) with comma-separated values.

-- skip.header.line.count = 1:
-- Skips the header row in the CSV file to prevent parsing errors.

-- IMPORTANT:
-- Update the LOCATION path, {hdfs_dataset_path} based on your HDFS directory
-- Example: 
-- LOCATION '/user/student5/project_assignment/Dataset';

CREATE EXTERNAL TABLE nyctaxi_zone (
    LocationID INT,
    Borough STRING,
    Zone STRING,
    service_zone STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{hdfs_dataset_path}'
TBLPROPERTIES (
    'skip.header.line.count'='1'
);
