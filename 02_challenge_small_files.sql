/*
================================================================================
  CHALLENGE 1: SMALL FILE ACCUMULATION
================================================================================
  PROBLEM:
    Streaming data creates millions of tiny Parquet files that paralyze query
    planning. Each micro-batch commit writes a new small file. Over time, the
    table has thousands of files under 1MB, causing excessive file-open overhead
    and slow metadata scans.

  SNOWFLAKE MITIGATION:
    1. Automatic Data Compaction (enabled by default) - Merges small files
       into larger, optimally-sized files in the background.
    2. TARGET_FILE_SIZE parameter - Controls the target output file size.
    3. Serverless execution - No user-managed Spark cluster needed.

  KEY PARAMETERS:
    - ENABLE_DATA_COMPACTION = TRUE (default)
    - TARGET_FILE_SIZE = 'AUTO' | '16MB' | '32MB' | '64MB' | '128MB'

  PREREQUISITE: Run 00_setup_infrastructure.sql and 01_generate_synthetic_data.sql
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Observe the small-file problem
-- ============================================

-- Check current file metrics for transactions
-- After 50 micro-batch inserts, we have many small files
SELECT
    TABLE_NAME,
    ACTIVE_BYTES,
    TIME_TRAVEL_BYTES,
    RETAINED_FOR_CLONE_BYTES,
    TABLE_CREATED,
    CATALOG_CONVERTED
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_CATALOG = 'ICEBERG_CHALLENGES_DB'
  AND TABLE_NAME = 'TRANSACTIONS'
  AND TABLE_SCHEMA = 'DEMO'
ORDER BY TABLE_CREATED DESC
LIMIT 5;

-- Get Iceberg table info to see current metadata location
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_CHALLENGES_DB.DEMO.TRANSACTIONS');

-- Check current TARGET_FILE_SIZE (set to 16MB in data gen to show the problem)
SHOW PARAMETERS LIKE 'TARGET_FILE_SIZE' IN TABLE transactions;


-- ============================================
-- STEP 2: Apply Snowflake's automatic mitigation
-- ============================================

-- Option A: Set TARGET_FILE_SIZE to AUTO (Snowflake optimizes dynamically)
ALTER ICEBERG TABLE transactions
  SET TARGET_FILE_SIZE = 'AUTO';

-- Option B: Or set a specific target (128MB is optimal for large analytical tables)
-- ALTER ICEBERG TABLE transactions SET TARGET_FILE_SIZE = '128MB';

-- Verify data compaction is enabled (it is by default)
SHOW PARAMETERS LIKE 'ENABLE_DATA_COMPACTION' IN TABLE transactions;

-- Explicitly ensure it's on (idempotent)
ALTER ICEBERG TABLE transactions
  SET ENABLE_DATA_COMPACTION = TRUE;


-- ============================================
-- STEP 3: Demonstrate the fix in action
-- ============================================

-- Insert more micro-batches AFTER setting AUTO target size
-- Snowflake will now write larger files from the start
CALL simulate_transaction_stream(10, 50000);

-- The background compaction service will also start merging existing small files
-- This happens asynchronously (typically within minutes)


-- ============================================
-- STEP 4: Verify mitigation - Monitor compaction
-- ============================================

-- Query the Iceberg storage optimization history
-- Shows compaction jobs, bytes processed, and credits used
SELECT
    TABLE_NAME,
    START_TIME,
    END_TIME,
    CREDITS_USED,
    NUM_BYTES_SCANNED,
    NUM_ROWS_WRITTEN
FROM SNOWFLAKE.ACCOUNT_USAGE.ICEBERG_STORAGE_OPTIMIZATION_HISTORY
WHERE DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
  AND TABLE_NAME = 'TRANSACTIONS'
ORDER BY START_TIME DESC
LIMIT 10;

-- Compare storage before/after (may need to wait a few minutes for compaction)
SELECT
    TABLE_NAME,
    ACTIVE_BYTES,
    TIME_TRAVEL_BYTES
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_CATALOG = 'ICEBERG_CHALLENGES_DB'
  AND TABLE_NAME = 'TRANSACTIONS'
  AND TABLE_SCHEMA = 'DEMO'
ORDER BY TABLE_CREATED DESC
LIMIT 1;

-- Verify the new TARGET_FILE_SIZE setting
SHOW PARAMETERS LIKE 'TARGET_FILE_SIZE' IN TABLE transactions;


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg, you must:
    - Deploy and manage Apache Spark clusters for compaction
    - Schedule compaction jobs (Airflow, cron, etc.)
    - Tune file size parameters manually
    - Monitor and restart failed compaction jobs
  
  In Snowflake:
    - Data compaction is AUTOMATIC and SERVERLESS
    - TARGET_FILE_SIZE = 'AUTO' adapts to your workload pattern
    - No infrastructure to manage
    - Costs tracked transparently in ICEBERG_STORAGE_OPTIMIZATION_HISTORY
================================================================================
*/
