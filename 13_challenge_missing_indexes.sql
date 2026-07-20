/*
================================================================================
  CHALLENGE 12: MISSING PLATFORM INDEXES
================================================================================
  PROBLEM:
    Native performance accelerators like search optimization, auto-clustering,
    and materialized views do NOT apply to Iceberg tables in most platforms:
    - No clustering/Z-ordering without manual Spark rewrite_data_files
    - No search optimization or skip indexes
    - Full table scans for point lookups on large tables
    - Query planning cannot leverage platform-native statistics
    - Users must manually partition data for query performance

  SNOWFLAKE MITIGATION:
    Snowflake provides NATIVE performance optimization on Iceberg tables:
    1. Automatic Clustering (CLUSTER BY) - Reorganizes data by query patterns
    2. Intelligent partition pruning using Snowflake micro-partition stats
    3. Query acceleration service compatibility
    4. Metadata-based scan optimization

  KEY FEATURES:
    - CLUSTER BY (col1, col2) on Iceberg tables
    - Automatic re-clustering as data changes
    - Query profile shows partition pruning effectiveness
    - Works identically to native Snowflake table clustering

  PREREQUISITE: Run 00 and 01 first (needs market_data and transactions tables)
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Show poor scan performance WITHOUT clustering
-- ============================================

-- The market_data table has 2M rows without clustering.
-- Point queries must scan everything.

-- Baseline: Query for a specific symbol and date range (no clustering)
ALTER SESSION SET QUERY_TAG = 'INDEX_NO_CLUSTER';

SELECT
    symbol,
    DATE_TRUNC('hour', tick_timestamp) AS hour,
    COUNT(*) AS tick_count,
    AVG(price) AS avg_price,
    MAX(price) - MIN(price) AS price_spread,
    SUM(volume) AS total_volume
FROM market_data
WHERE symbol = 'AAPL'
  AND tick_timestamp BETWEEN '2024-03-01' AND '2024-03-31'
GROUP BY 1, 2
ORDER BY hour;

-- Check how many partitions were scanned (expecting most/all)
SELECT
    QUERY_TAG,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    BYTES_SCANNED / (1024*1024) AS mb_scanned,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROUND(PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) * 100, 1) AS pct_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG = 'INDEX_NO_CLUSTER'
  AND QUERY_TYPE = 'SELECT'
ORDER BY START_TIME DESC
LIMIT 1;

-- Without clustering: likely scanning 80-100% of partitions


-- ============================================
-- STEP 2: Add Automatic Clustering
-- ============================================

-- Apply clustering on the columns most frequently filtered on
-- For market_data: symbol (point lookups) + tick_timestamp (range scans)
ALTER ICEBERG TABLE market_data
  CLUSTER BY (symbol, tick_timestamp);

-- Snowflake will now automatically:
-- 1. Reorganize existing data files by (symbol, tick_timestamp) in background
-- 2. Write new data in clustered order
-- 3. Maintain clustering as new data arrives
-- This is FULLY AUTOMATIC - no Spark rewrite_data_files needed!

-- Check clustering status
SELECT SYSTEM$CLUSTERING_INFORMATION('market_data', '(symbol, tick_timestamp)');

-- Also cluster the transactions table (common queries filter by region + timestamp)
ALTER ICEBERG TABLE transactions
  CLUSTER BY (region, txn_timestamp);

-- Verify clustering is set
SHOW ICEBERG TABLES LIKE 'market_data' IN SCHEMA DEMO;
SHOW ICEBERG TABLES LIKE 'transactions' IN SCHEMA DEMO;


-- ============================================
-- STEP 3: Re-run query AFTER clustering takes effect
-- ============================================

-- NOTE: Auto-clustering runs in the background. For large tables, 
-- it may take minutes to hours to fully reorganize.
-- For this demo, we can see immediate benefits on new writes.

-- Wait a moment for initial clustering to begin, then re-query
-- (In a live demo, you may want to insert fresh data and query that)

ALTER SESSION SET QUERY_TAG = 'INDEX_WITH_CLUSTER';

SELECT
    symbol,
    DATE_TRUNC('hour', tick_timestamp) AS hour,
    COUNT(*) AS tick_count,
    AVG(price) AS avg_price,
    MAX(price) - MIN(price) AS price_spread,
    SUM(volume) AS total_volume
FROM market_data
WHERE symbol = 'AAPL'
  AND tick_timestamp BETWEEN '2024-03-01' AND '2024-03-31'
GROUP BY 1, 2
ORDER BY hour;

-- Compare partition pruning before/after
SELECT
    QUERY_TAG,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    BYTES_SCANNED / (1024*1024) AS mb_scanned,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROUND(PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) * 100, 1) AS pct_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG IN ('INDEX_NO_CLUSTER', 'INDEX_WITH_CLUSTER')
  AND QUERY_TYPE = 'SELECT'
ORDER BY QUERY_TAG;

-- Expected after clustering takes effect:
-- PARTITIONS_SCANNED drops significantly (e.g., from 100% to 5-15%)
-- BYTES_SCANNED reduces proportionally
-- Query time improves dramatically


-- ============================================
-- STEP 4: Demonstrate clustering on transactions table
-- ============================================

-- Query transactions for a specific region and time range
ALTER SESSION SET QUERY_TAG = 'INDEX_TXN_CLUSTERED';

SELECT
    txn_type,
    txn_status,
    COUNT(*) AS cnt,
    SUM(amount) AS total_amount,
    AVG(fraud_score) AS avg_fraud
FROM transactions
WHERE region = 'EMEA'
  AND txn_timestamp BETWEEN '2024-04-01' AND '2024-04-30'
  AND amount > 10000
GROUP BY 1, 2
ORDER BY total_amount DESC;

-- Check partition pruning effectiveness
SELECT
    QUERY_TAG,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    BYTES_SCANNED / (1024*1024) AS mb_scanned,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROUND(PARTITIONS_SCANNED / NULLIF(PARTITIONS_TOTAL, 0) * 100, 1) AS pct_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG = 'INDEX_TXN_CLUSTERED'
  AND QUERY_TYPE = 'SELECT'
ORDER BY START_TIME DESC
LIMIT 1;


-- ============================================
-- STEP 5: Monitor clustering depth and cost
-- ============================================

-- Clustering depth (lower = better organized)
SELECT SYSTEM$CLUSTERING_DEPTH('market_data', '(symbol, tick_timestamp)');
SELECT SYSTEM$CLUSTERING_DEPTH('transactions', '(region, txn_timestamp)');

-- Clustering information (detailed statistics)
SELECT SYSTEM$CLUSTERING_INFORMATION('market_data', '(symbol, tick_timestamp)');

-- Auto-clustering credit consumption (separate from compaction)
SELECT
    TABLE_NAME,
    START_TIME,
    END_TIME,
    CREDITS_USED,
    NUM_BYTES_RECLUSTERED / (1024*1024) AS mb_reclustered
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
  AND TABLE_NAME IN ('MARKET_DATA', 'TRANSACTIONS')
ORDER BY START_TIME DESC
LIMIT 20;

ALTER SESSION UNSET QUERY_TAG;


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg (no native indexes):
    - Must run Spark `rewrite_data_files` with sort-order for clustering
    - Z-ordering requires external tools (Delta Lake has it, Iceberg does not)
    - No skip indexes or search optimization
    - Must manually manage partition layout for performance
    - Re-clustering after data changes requires scheduled Spark jobs
    - Point lookups always scan large portions of data
  
  In Snowflake (native performance optimization):
    - CLUSTER BY works directly on Iceberg tables
    - Automatic re-clustering maintains order as data changes (serverless)
    - Partition pruning leverages Snowflake's metadata statistics
    - Up to 90%+ scan reduction for well-clustered queries
    - No Spark infrastructure needed
    - Cost tracked in AUTOMATIC_CLUSTERING_HISTORY
    - Works identically to native Snowflake table clustering
================================================================================
*/
