#  NYC Taxi Hive Performance Analysis

This project evaluates the performance of different Hive table configurations using the Hadoop ecosystem. The goal is to analyze how storage format, table type, partitioning, and bucketing impact query execution time and storage efficiency by using the NYC Yellow Taxi January 2025 dataset.

##  Objective
To analyze how Hive table design affects query performance, comparing:

- External vs Internal tables
- Textfile vs Parquet formats
- Partitioned tables
- Bucketed tables
- Partitioned + Bucketed tables

##  Dataset
- **Primary dataset**: [NYC Taxi Trip Data (Jan 2025)](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet)  
- **Size**: ~3.4 million rows
- **Secondary dataset**: [Taxi Zone Lookup Table](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)  


## Project Workflow
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

### Example Query (External Textfile)

```sql
SELECT
    z.Borough AS borough,
    COUNT(*) AS total_trips,
    ROUND(AVG(t.total_amount), 2) AS avg_fare,
    CAST(COALESCE(SUM(t.total_amount), 0) AS DECIMAL(15,2)) AS total_fare
FROM nyctaxi_external_textfile t
JOIN nyctaxi_zone z
    ON t.PULocationID = z.LocationID
WHERE to_date(t.pickup_datetime)
      BETWEEN '2025-01-01' AND '2025-01-07'
GROUP BY z.Borough
ORDER BY total_trips DESC;
```

