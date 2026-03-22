# 🚖 NYC Taxi Hive Performance Analysis

This project evaluates Apache Hive query performance across different table configurations using the NYC Yellow Taxi January 2025 dataset.

## 📌 Objective
To analyze how Hive table design affects query performance, comparing:

- External vs Internal tables
- Textfile vs Parquet formats
- Partitioned tables
- Bucketed tables
- Partitioned + Bucketed tables

## 🛠️ Technologies Used
- Hadoop (HDFS, YARN)
- Apache Hive
- Hue Interface
- Parquet
- Textfile

## 📊 Dataset
- NYC Yellow Taxi Trip Records (January 2025)
- Taxi Zone Lookup Table
- ~3.4 million records in main dataset

## 🔄 Workflow
1. Upload dataset to HDFS
2. Inspect schema using Hive
3. Convert timestamp fields for Hive compatibility
4. Create raw external table
5. Create multiple table configurations:
   - External (Parquet & Textfile)
   - Internal (Parquet & Textfile)
   - Partitioned tables
   - Bucketed tables
   - Partitioned + Bucketed tables
6. Execute same query across all tables
7. Compare performance (runtime & storage)

## 🧪 Key Query (Benchmark)
```sql
SELECT 
    z.Borough,
    COUNT(*) AS total_trips,
    ROUND(AVG(t.fare_amount), 2) AS avg_fare,
    ROUND(AVG(t.tip_amount), 2) AS avg_tip
FROM trips_table t
JOIN taxi_zone_lookup z
ON t.PULocationID = z.LocationID
GROUP BY z.Borough;
