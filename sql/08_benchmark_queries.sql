-- ==============================
-- Query Design & Performance Evaluation
-- ==============================
-- Description:
-- A consistent aggregation query is used across all table configurations
-- to ensure fair and comparable performance evaluation.

-- Performance evaluation:
-- - Query executed 3 times per configuration
-- - Average execution time used for comparison
-- - Storage size collected using SHOW TABLE EXTENDED

-- Purpose:
-- To evaluate trade-offs between:
-- - Table type (external vs internal)
-- - File format (Textfile vs Parquet)
-- - Optimization techniques (partitioning, bucketing)

-- IMPORTANT:
-- Replace 'table_name' with the target table for each query execution.

SELECT
    z.Borough AS borough,
    COUNT(*) AS total_trips,
    ROUND(AVG(t.total_amount), 2) AS avg_fare,
    CAST(COALESCE(SUM(t.total_amount), 0) AS DECIMAL(15,2)) AS total_fare
FROM table_name t
JOIN nyctaxi_zone z
    ON t.PULocationID = z.LocationID
WHERE to_date(t.pickup_datetime)
      BETWEEN '2025-01-01' AND '2025-01-07'
GROUP BY z.Borough
ORDER BY total_trips DESC;

-- ==============================
-- Table Metadata & Storage Size
-- ==============================
-- Description:
-- Retrieves table metadata and storage information
-- used to compare storage efficiency across configurations.

SHOW TABLE EXTENDED LIKE 'table_name';
