/*
================================================================================
  STORAGE MODE COMPARISON: External Volume (S3) vs SNOWFLAKE_MANAGED
================================================================================
  PURPOSE:  Side-by-side benchmark comparing two Snowflake Iceberg storage modes:
  
  MODE 1: External Volume (Customer-managed S3)
    - Data stored in YOUR S3 bucket
    - You pay AWS S3 storage costs directly
    - Full interoperability with Spark/Trino/Flink via open Parquet files
    - You manage bucket lifecycle, encryption, cross-region replication
    - Compaction is billed as Snowflake serverless credits

  MODE 2: SNOWFLAKE_MANAGED Storage
    - Data stored in Snowflake-managed internal storage
    - Storage billed as part of Snowflake (like native tables)
    - Data compaction is BUNDLED (zero additional cost when only Snowflake writes)
    - Still produces standard Iceberg metadata for external readers
    - Simplest operational model

  BENCHMARKS:
    - Table creation and bulk load time
    - DML latency (INSERT, UPDATE, DELETE, MERGE)
    - Query scan performance
    - Storage metrics comparison
    - Compaction cost comparison

  PREREQUISITE: Run 00_setup_infrastructure.sql first
  ESTIMATED TIME: 5-8 minutes
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- =============================================================================
-- SETUP: Create identical tables on BOTH storage modes
-- =============================================================================

-- Table on External Volume (S3) - your bucket, your storage costs
CREATE OR REPLACE ICEBERG TABLE bench_external_vol (
    txn_id          STRING,
    account_id      STRING,
    txn_type        STRING,
    amount          NUMBER(15,2),
    currency        STRING,
    txn_timestamp   TIMESTAMP_NTZ,
    region          STRING,
    risk_flag       BOOLEAN,
    fraud_score     FLOAT
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'benchmark/external_vol'
TARGET_FILE_SIZE = 'AUTO'
ICEBERG_VERSION = 3;

-- Table on Snowflake Managed Storage - Snowflake handles everything
CREATE OR REPLACE ICEBERG TABLE bench_managed_storage (
    txn_id          STRING,
    account_id      STRING,
    txn_type        STRING,
    amount          NUMBER(15,2),
    currency        STRING,
    txn_timestamp   TIMESTAMP_NTZ,
    region          STRING,
    risk_flag       BOOLEAN,
    fraud_score     FLOAT
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'snowflake_managed'
TARGET_FILE_SIZE = 'AUTO'
ICEBERG_VERSION = 3;


-- =============================================================================
-- BENCHMARK 1: BULK INSERT PERFORMANCE
-- =============================================================================

-- Load 1M rows into External Volume table
ALTER SESSION SET QUERY_TAG = 'BENCH_INSERT_EXTERNAL';

INSERT INTO bench_external_vol
SELECT
    UUID_STRING(),
    'ACCT_' || LPAD(UNIFORM(1, 100000, RANDOM())::VARCHAR, 8, '0'),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'PAYMENT' WHEN 2 THEN 'TRANSFER'
        WHEN 3 THEN 'TRADE_BUY' WHEN 4 THEN 'DEPOSIT'
    END,
    ROUND(UNIFORM(10, 100000, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2),
    CASE UNIFORM(1, 3, RANDOM()) WHEN 1 THEN 'USD' WHEN 2 THEN 'EUR' WHEN 3 THEN 'GBP' END,
    DATEADD(second, UNIFORM(0, 15552000, RANDOM()), '2024-01-01'::TIMESTAMP_NTZ),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'AMERICAS' WHEN 2 THEN 'EMEA' WHEN 3 THEN 'APAC' WHEN 4 THEN 'LATAM'
    END,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 5 THEN TRUE ELSE FALSE END,
    ROUND(UNIFORM(0, 100, RANDOM()) / 100.0, 4)
FROM TABLE(GENERATOR(ROWCOUNT => 1000000));

-- Load 1M rows into Managed Storage table
ALTER SESSION SET QUERY_TAG = 'BENCH_INSERT_MANAGED';

INSERT INTO bench_managed_storage
SELECT
    UUID_STRING(),
    'ACCT_' || LPAD(UNIFORM(1, 100000, RANDOM())::VARCHAR, 8, '0'),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'PAYMENT' WHEN 2 THEN 'TRANSFER'
        WHEN 3 THEN 'TRADE_BUY' WHEN 4 THEN 'DEPOSIT'
    END,
    ROUND(UNIFORM(10, 100000, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2),
    CASE UNIFORM(1, 3, RANDOM()) WHEN 1 THEN 'USD' WHEN 2 THEN 'EUR' WHEN 3 THEN 'GBP' END,
    DATEADD(second, UNIFORM(0, 15552000, RANDOM()), '2024-01-01'::TIMESTAMP_NTZ),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'AMERICAS' WHEN 2 THEN 'EMEA' WHEN 3 THEN 'APAC' WHEN 4 THEN 'LATAM'
    END,
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 5 THEN TRUE ELSE FALSE END,
    ROUND(UNIFORM(0, 100, RANDOM()) / 100.0, 4)
FROM TABLE(GENERATOR(ROWCOUNT => 1000000));

-- Compare INSERT performance
SELECT
    QUERY_TAG,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    ROWS_INSERTED,
    BYTES_WRITTEN_TO_RESULT
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG IN ('BENCH_INSERT_EXTERNAL', 'BENCH_INSERT_MANAGED')
  AND QUERY_TYPE = 'INSERT'
ORDER BY QUERY_TAG;


-- =============================================================================
-- BENCHMARK 2: QUERY SCAN PERFORMANCE
-- =============================================================================

-- Query External Volume table
ALTER SESSION SET QUERY_TAG = 'BENCH_QUERY_EXTERNAL';

SELECT
    region,
    txn_type,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount,
    AVG(fraud_score) AS avg_fraud_score,
    COUNT_IF(risk_flag) AS flagged_count
FROM bench_external_vol
WHERE txn_timestamp >= '2024-03-01'
  AND amount > 1000
GROUP BY region, txn_type
ORDER BY total_amount DESC;

-- Query Managed Storage table
ALTER SESSION SET QUERY_TAG = 'BENCH_QUERY_MANAGED';

SELECT
    region,
    txn_type,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount,
    AVG(fraud_score) AS avg_fraud_score,
    COUNT_IF(risk_flag) AS flagged_count
FROM bench_managed_storage
WHERE txn_timestamp >= '2024-03-01'
  AND amount > 1000
GROUP BY region, txn_type
ORDER BY total_amount DESC;

-- Compare query performance
SELECT
    QUERY_TAG,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    BYTES_SCANNED / (1024*1024) AS mb_scanned,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    ROWS_PRODUCED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG IN ('BENCH_QUERY_EXTERNAL', 'BENCH_QUERY_MANAGED')
  AND QUERY_TYPE = 'SELECT'
ORDER BY QUERY_TAG;


-- =============================================================================
-- BENCHMARK 3: DML (UPDATE) PERFORMANCE
-- =============================================================================

-- Enable merge-on-read for both tables
ALTER ICEBERG TABLE bench_external_vol SET ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ENABLED';
ALTER ICEBERG TABLE bench_managed_storage SET ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ENABLED';

-- UPDATE on External Volume
ALTER SESSION SET QUERY_TAG = 'BENCH_UPDATE_EXTERNAL';

UPDATE bench_external_vol
SET risk_flag = TRUE, fraud_score = 0.99
WHERE region = 'AMERICAS' AND amount > 50000;

-- UPDATE on Managed Storage
ALTER SESSION SET QUERY_TAG = 'BENCH_UPDATE_MANAGED';

UPDATE bench_managed_storage
SET risk_flag = TRUE, fraud_score = 0.99
WHERE region = 'AMERICAS' AND amount > 50000;

-- Compare UPDATE performance
SELECT
    QUERY_TAG,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    ROWS_UPDATED,
    BYTES_SCANNED / (1024*1024) AS mb_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG IN ('BENCH_UPDATE_EXTERNAL', 'BENCH_UPDATE_MANAGED')
  AND QUERY_TYPE = 'UPDATE'
ORDER BY QUERY_TAG;


-- =============================================================================
-- BENCHMARK 4: DELETE PERFORMANCE
-- =============================================================================

ALTER SESSION SET QUERY_TAG = 'BENCH_DELETE_EXTERNAL';
DELETE FROM bench_external_vol WHERE fraud_score > 0.95 AND risk_flag = TRUE;

ALTER SESSION SET QUERY_TAG = 'BENCH_DELETE_MANAGED';
DELETE FROM bench_managed_storage WHERE fraud_score > 0.95 AND risk_flag = TRUE;

-- Compare DELETE performance
SELECT
    QUERY_TAG,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    ROWS_DELETED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG IN ('BENCH_DELETE_EXTERNAL', 'BENCH_DELETE_MANAGED')
  AND QUERY_TYPE = 'DELETE'
ORDER BY QUERY_TAG;


-- =============================================================================
-- BENCHMARK 5: STORAGE METRICS COMPARISON
-- =============================================================================

-- Storage footprint for both tables
SELECT
    TABLE_NAME,
    ACTIVE_BYTES / (1024*1024) AS active_mb,
    TIME_TRAVEL_BYTES / (1024*1024) AS time_travel_mb,
    (ACTIVE_BYTES + TIME_TRAVEL_BYTES) / (1024*1024) AS total_mb
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_CATALOG = 'ICEBERG_CHALLENGES_DB'
  AND TABLE_SCHEMA = 'DEMO'
  AND TABLE_NAME IN ('BENCH_EXTERNAL_VOL', 'BENCH_MANAGED_STORAGE')
ORDER BY TABLE_NAME;

-- Iceberg metadata info
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_CHALLENGES_DB.DEMO.BENCH_EXTERNAL_VOL') AS external_vol_metadata;
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_CHALLENGES_DB.DEMO.BENCH_MANAGED_STORAGE') AS managed_storage_metadata;


-- =============================================================================
-- BENCHMARK 6: COMPACTION COST COMPARISON
-- =============================================================================

-- For External Volume: compaction is billed as serverless credits
-- For Managed Storage: compaction is BUNDLED (free when only Snowflake writes!)

SELECT
    TABLE_NAME,
    COUNT(*) AS compaction_jobs,
    SUM(CREDITS_USED) AS total_credits,
    SUM(NUM_BYTES_SCANNED) / (1024*1024) AS mb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.ICEBERG_STORAGE_OPTIMIZATION_HISTORY
WHERE DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
  AND TABLE_NAME IN ('BENCH_EXTERNAL_VOL', 'BENCH_MANAGED_STORAGE')
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;


-- =============================================================================
-- SUMMARY: SIDE-BY-SIDE COMPARISON
-- =============================================================================

/*
  ┌──────────────────────────────┬────────────────────────────┬────────────────────────────┐
  │ Feature                      │ External Volume (S3)       │ SNOWFLAKE_MANAGED          │
  ├──────────────────────────────┼────────────────────────────┼────────────────────────────┤
  │ Storage Location             │ Your S3 bucket             │ Snowflake-managed storage  │
  │ Storage Billing              │ AWS S3 costs (your bill)   │ Snowflake storage credits  │
  │ Compaction Cost              │ Serverless credits (billed)│ BUNDLED (free for SF-only) │
  │ External Engine Access       │ Direct S3 read (open)      │ Via Iceberg REST Catalog   │
  │ Cross-cloud Replication      │ You manage (S3 replication)│ Snowflake replication      │
  │ Encryption                   │ Your KMS keys              │ Snowflake-managed          │
  │ Write Performance            │ S3 PUT latency             │ Optimized internal writes  │
  │ Read Performance             │ S3 GET latency             │ Optimized internal reads   │
  │ Operational Complexity       │ Manage bucket, IAM, etc.   │ Zero infrastructure        │
  │ Data Sovereignty             │ Full control (your region) │ Snowflake region           │
  │ Open Format Guarantee        │ Parquet files on your S3   │ Parquet in managed storage │
  ├──────────────────────────────┼────────────────────────────┼────────────────────────────┤
  │ BEST FOR                     │ Multi-engine lakehouse,    │ Snowflake-primary workloads│
  │                              │ data sovereignty, existing │ wanting Iceberg interop    │
  │                              │ S3 data lakes              │ with zero ops overhead     │
  └──────────────────────────────┴────────────────────────────┴────────────────────────────┘
  
  RECOMMENDATION:
  - Use External Volume when: multiple engines need direct file access, data
    sovereignty requirements, existing S3-based data lake infrastructure
  - Use SNOWFLAKE_MANAGED when: Snowflake is the primary compute engine, want
    zero infrastructure management, bundled compaction savings matter
*/


-- =============================================================================
-- CONSOLIDATED BENCHMARK RESULTS
-- =============================================================================

-- All benchmarks in one view
SELECT
    QUERY_TAG,
    QUERY_TYPE,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    ROWS_PRODUCED + ROWS_INSERTED + ROWS_UPDATED + ROWS_DELETED AS rows_affected,
    BYTES_SCANNED / (1024*1024) AS mb_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG LIKE 'BENCH_%'
  AND QUERY_TYPE IN ('INSERT', 'SELECT', 'UPDATE', 'DELETE')
ORDER BY
    CASE
        WHEN QUERY_TAG LIKE '%INSERT%' THEN 1
        WHEN QUERY_TAG LIKE '%QUERY%' THEN 2
        WHEN QUERY_TAG LIKE '%UPDATE%' THEN 3
        WHEN QUERY_TAG LIKE '%DELETE%' THEN 4
    END,
    QUERY_TAG;

-- Clean up
ALTER SESSION UNSET QUERY_TAG;
DROP ICEBERG TABLE IF EXISTS bench_external_vol;
DROP ICEBERG TABLE IF EXISTS bench_managed_storage;


/*
================================================================================
  Next: Run 02_challenge_small_files.sql to begin the 13 challenge demonstrations.
================================================================================
*/
