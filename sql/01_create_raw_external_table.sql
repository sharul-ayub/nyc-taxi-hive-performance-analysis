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
LOCATION '/user/student5/project_assignment/Dataset';



---

### ✔ Option 3 (Best for portfolio 🔥)

Make it configurable:

```sql
-- Replace with your HDFS dataset path
-- Example: /user/your_username/project_assignment/Dataset
LOCATION '${hdfs_dataset_path}';
