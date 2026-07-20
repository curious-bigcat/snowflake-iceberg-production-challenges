/*
================================================================================
  CHALLENGE 4: COPY-ON-WRITE LATENCY
================================================================================
  PROBLEM:
    In Copy-on-Write (COW) mode, updating even a single row requires rewriting
    the ENTIRE Parquet data file containing that row. For large files (128MB+),
    this means:
    - Massive write amplification (update 1 row = rewrite 1M rows)
    - High latency for UPDATE/DELETE/MERGE operations
    - Increased storage costs during rewrite (old + new file exist temporarily)

  SNOWFLAKE MITIGATION:
    Iceberg v3 with Deletion Vectors (Merge-on-Read):
    - Instead of rewriting data files, Snowflake writes small "deletion vector"
      files marking deleted/updated rows
    - Dramatically reduces write latency
    - Controlled via ICEBERG_MERGE_ON_READ_BEHAVIOR parameter
    - Smart heuristic: auto-switches between MOR and COW based on % of rows affected

  KEY PARAMETERS:
    - ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ENABLED' | 'DISABLED' | 'AUTO'
    - ICEBERG_VERSION_DEFAULT = 3 (required for deletion vectors)

  PREREQUISITE: Run 00 and 01 first
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Demonstrate Copy-on-Write latency
-- ============================================

-- Create a table explicitly in COPY-ON-WRITE mode for comparison
CREATE OR REPLACE ICEBERG TABLE orders_cow_test (
    order_id        NUMBER(38,0),
    customer_id     STRING,
    order_date      DATE,
    status          STRING,
    total_amount    NUMBER(12,2),
    region          STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'orders_cow_test'
TARGET_FILE_SIZE = '128MB';

-- Force Copy-on-Write mode
ALTER ICEBERG TABLE orders_cow_test
  SET ICEBERG_MERGE_ON_READ_BEHAVIOR = 'DISABLED';

-- Load substantial data (100K rows to create large files)
INSERT INTO orders_cow_test
SELECT
    SEQ4() + 1,
    'CUST_' || LPAD(UNIFORM(1, 1000, RANDOM())::VARCHAR, 4, '0'),
    DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'PENDING' WHEN 2 THEN 'PROCESSING'
        WHEN 3 THEN 'SHIPPED' WHEN 4 THEN 'DELIVERED'
    END,
    ROUND(UNIFORM(10, 5000, RANDOM()) + RANDOM() / 1e12, 2),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'us-east-1' WHEN 2 THEN 'us-west-2'
        WHEN 3 THEN 'eu-west-1' WHEN 4 THEN 'ap-south-1'
    END
FROM TABLE(GENERATOR(ROWCOUNT => 100000));

-- Now perform a small UPDATE in COW mode (rewrites entire files!)
-- Time this operation:
ALTER SESSION SET QUERY_TAG = 'COW_UPDATE_TEST';

UPDATE orders_cow_test
SET status = 'CANCELLED'
WHERE order_id IN (1, 2, 3, 4, 5);  -- Only 5 rows!

-- Check query duration in history
SELECT
    QUERY_ID,
    QUERY_TAG,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    BYTES_WRITTEN_TO_RESULT,
    ROWS_PRODUCED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG = 'COW_UPDATE_TEST'
ORDER BY START_TIME DESC
LIMIT 1;


-- ============================================
-- STEP 2: Enable Merge-on-Read with Deletion Vectors
-- ============================================

-- Create identical table with Merge-on-Read enabled (Iceberg v3)
CREATE OR REPLACE ICEBERG TABLE orders_mor_test (
    order_id        NUMBER(38,0),
    customer_id     STRING,
    order_date      DATE,
    status          STRING,
    total_amount    NUMBER(12,2),
    region          STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'orders_mor_test'
TARGET_FILE_SIZE = '128MB'
ICEBERG_VERSION = 3;  -- Required for deletion vectors

-- Enable Merge-on-Read
ALTER ICEBERG TABLE orders_mor_test
  SET ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ENABLED';

-- Load same amount of data
INSERT INTO orders_mor_test
SELECT
    SEQ4() + 1,
    'CUST_' || LPAD(UNIFORM(1, 1000, RANDOM())::VARCHAR, 4, '0'),
    DATEADD(day, -UNIFORM(1, 365, RANDOM()), CURRENT_DATE()),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'PENDING' WHEN 2 THEN 'PROCESSING'
        WHEN 3 THEN 'SHIPPED' WHEN 4 THEN 'DELIVERED'
    END,
    ROUND(UNIFORM(10, 5000, RANDOM()) + RANDOM() / 1e12, 2),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'us-east-1' WHEN 2 THEN 'us-west-2'
        WHEN 3 THEN 'eu-west-1' WHEN 4 THEN 'ap-south-1'
    END
FROM TABLE(GENERATOR(ROWCOUNT => 100000));


-- ============================================
-- STEP 3: Compare - Same update, much faster
-- ============================================

ALTER SESSION SET QUERY_TAG = 'MOR_UPDATE_TEST';

-- Same 5-row update, but now writes only a tiny deletion vector file
UPDATE orders_mor_test
SET status = 'CANCELLED'
WHERE order_id IN (1, 2, 3, 4, 5);

-- Compare durations
SELECT
    QUERY_TAG,
    QUERY_ID,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    BYTES_SCANNED,
    ROWS_PRODUCED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG IN ('COW_UPDATE_TEST', 'MOR_UPDATE_TEST')
  AND QUERY_TYPE = 'UPDATE'
ORDER BY START_TIME DESC
LIMIT 5;


-- ============================================
-- STEP 4: Understand the heuristic
-- ============================================

-- Snowflake's AUTO mode uses smart heuristics:
-- - If < 5% of rows in a file are affected -> uses MOR (deletion vectors)
-- - If >= 5% of rows in a file are affected -> falls back to COW (more efficient)
-- - If file is smaller than ~1.6MB -> uses COW (deletion vector overhead not worth it)

-- Show current parameter setting
SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' IN TABLE orders_mor_test;

-- Demonstrate large update (>5% of file) - Snowflake auto-switches to COW
ALTER SESSION SET QUERY_TAG = 'MOR_LARGE_UPDATE';

UPDATE orders_mor_test
SET status = 'ARCHIVED'
WHERE region = 'us-east-1';  -- ~25% of rows - Snowflake will use COW here

-- Clean up test tables
DROP ICEBERG TABLE IF EXISTS orders_cow_test;
DROP ICEBERG TABLE IF EXISTS orders_mor_test;

ALTER SESSION UNSET QUERY_TAG;


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg with COW (default):
    - Updating 5 rows in a 128MB file = rewrite entire 128MB file
    - Write amplification of 1000x-1,000,000x for small updates
    - Latency proportional to file size, not rows changed
  
  In Snowflake with Merge-on-Read (v3 deletion vectors):
    - Updating 5 rows = write a tiny deletion vector file (~KB)
    - Write latency proportional to ROWS CHANGED, not file size
    - Smart heuristic auto-switches to COW when bulk updating (>5% of file)
    - Best of both worlds with ICEBERG_MERGE_ON_READ_BEHAVIOR = 'AUTO'
================================================================================
*/
