#  NYC Taxi Hive Performance Analysis
<p align="justify">
This project evaluates the performance of different Hive table configurations using the Hadoop ecosystem. The goal is to analyze how storage format, table type, partitioning, and bucketing impact query execution time and storage efficiency by using the NYC Yellow Taxi January 2025 dataset.   
</p>

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
<p align="center">
  <img src="images/Workflow.png" alt="Workflow" width="500"/>
</p>
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

### Query Output (External Textfile)

<p align="center">
  <img src="images/query_output_external_textfile.png"/>
</p>

**Interpretation:**
- Manhattan records the highest number of trips, indicating the highest taxi demand.
- Queens shows a significantly higher average fare, suggesting longer trip distances.
- Other boroughs have lower trip volumes, with Staten Island being the lowest.

### Performance Comparison (Execution Time vs Storage)

<p align="center">
  <img src="images/performance_comparison.png" alt="Performance Comparison" width="500"/>
</p>

<p align="justify">
Across every table configuration, Parquet uses much less storage and runs faster than Textfile. For example, in the external table, switching from textfile (353.28 MB, 86.52 s) to Parquet (116.77 MB, 81.78 s) reduces storage by 236.51 MB and improves execution time by 4.74 s. The same trend appears for partitioned tables where partitioned textfile (286.99 MB, 65.85 s) versus partitioned Parquet (65.37 MB, 62.96 s) shows a storage reduction of 221.62 MB and faster execution by 2.89 s.
</p>
