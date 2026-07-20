/*
================================================================================
  CHALLENGE 5: MERGE-ON-READ PENALTIES
================================================================================
  PROBLEM:
    While Merge-on-Read (MOR) speeds up writes, it degrades READ performance
    because queries must merge deletion vectors/delete files with data files
    at read time. Without regular compaction:
    - Read latency grows linearly with number of delete files
    - Scan overhead multiplies as more deletions accumulate
    - Real-time dashboards suffer degrading response times

  SNOWFLAKE MITIGATION:
    1. Background compaction automatically merges deletion vectors back into
       data files, keeping read performance stable.
    2. Smart heuristic (AUTO mode) only uses MOR when beneficial - for bulk
       changes it automatically uses COW to avoid read penalty.
    3. Compaction is serverless and cost-effective.

  KEY INSIGHT:
    Snowflake solves the COW-vs-MOR tradeoff by making it NOT a tradeoff:
    - Fast writes via deletion vectors (MOR)
    - Fast reads via automatic background compaction
    - The "merge-on-read penalty" is temporary and self-healing

  PREREQUISITE: Run 00 and 01 first
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Create a table and accumulate deletion vectors
-- ============================================

-- Create a table with MOR enabled
CREATE OR REPLACE ICEBERG TABLE events_mor_demo (
    event_id        NUMBER(38,0),
    user_id         STRING,
    event_type      STRING,
    event_ts        TIMESTAMP_NTZ,
    payload_size    NUMBER(38,0),
    is_processed    BOOLEAN DEFAULT FALSE
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'events_mor_demo'
TARGET_FILE_SIZE = '64MB'
ICEBERG_VERSION = 3;

ALTER ICEBERG TABLE events_mor_demo
  SET ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ENABLED';

-- Load initial data (100K events)
INSERT INTO events_mor_demo
SELECT
    SEQ4() + 1,
    'user_' || LPAD(UNIFORM(1, 1000, RANDOM())::VARCHAR, 4, '0'),
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'page_view' WHEN 2 THEN 'click'
        WHEN 3 THEN 'purchase' WHEN 4 THEN 'signup' WHEN 5 THEN 'logout'
    END,
    DATEADD(second, UNIFORM(0, 2592000, RANDOM()), '2024-06-01'::TIMESTAMP_NTZ),
    UNIFORM(100, 10000, RANDOM()),
    FALSE
FROM TABLE(GENERATOR(ROWCOUNT => 100000));

-- Baseline read performance (before any deletions)
ALTER SESSION SET QUERY_TAG = 'MOR_READ_BASELINE';

SELECT event_type, COUNT(*) AS cnt, AVG(payload_size) AS avg_payload
FROM events_mor_demo
GROUP BY event_type;


-- ============================================
-- STEP 2: Simulate multiple small updates (accumulates delete vectors)
-- ============================================

-- Perform many small updates to accumulate deletion vectors
-- In OSS Iceberg without compaction, this would degrade reads over time

UPDATE events_mor_demo SET is_processed = TRUE WHERE event_id BETWEEN 1 AND 500;
UPDATE events_mor_demo SET is_processed = TRUE WHERE event_id BETWEEN 501 AND 1000;
UPDATE events_mor_demo SET is_processed = TRUE WHERE event_id BETWEEN 1001 AND 1500;
UPDATE events_mor_demo SET is_processed = TRUE WHERE event_id BETWEEN 1501 AND 2000;
UPDATE events_mor_demo SET is_processed = TRUE WHERE event_id BETWEEN 2001 AND 2500;

-- Delete some events (creates more deletion vectors)
DELETE FROM events_mor_demo WHERE event_type = 'logout' AND event_id < 5000;


-- ============================================
-- STEP 3: Show reads still perform well
-- ============================================

-- Read after accumulating deletion vectors
-- In Snowflake, background compaction resolves these automatically
ALTER SESSION SET QUERY_TAG = 'MOR_READ_AFTER_UPDATES';

SELECT event_type, COUNT(*) AS cnt, AVG(payload_size) AS avg_payload
FROM events_mor_demo
GROUP BY event_type;

-- Compare read performance before/after
SELECT
    QUERY_TAG,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    BYTES_SCANNED / (1024*1024) AS mb_scanned,
    ROWS_PRODUCED,
    PARTITIONS_SCANNED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG IN ('MOR_READ_BASELINE', 'MOR_READ_AFTER_UPDATES')
  AND QUERY_TYPE = 'SELECT'
ORDER BY QUERY_TAG;


-- ============================================
-- STEP 4: Verify - Compaction heals the read penalty
-- ============================================

-- Snowflake's background compaction will automatically:
-- 1. Identify data files with accumulated deletion vectors
-- 2. Rewrite those files with deletions applied
-- 3. Remove old deletion vector files
-- All without any user intervention!

-- Monitor compaction resolving deletion vectors
SELECT
    TABLE_NAME,
    START_TIME,
    END_TIME,
    CREDITS_USED,
    NUM_BYTES_SCANNED,
    NUM_ROWS_WRITTEN
FROM SNOWFLAKE.ACCOUNT_USAGE.ICEBERG_STORAGE_OPTIMIZATION_HISTORY
WHERE DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
  AND TABLE_NAME = 'EVENTS_MOR_DEMO'
ORDER BY START_TIME DESC
LIMIT 10;

-- The AUTO mode ensures optimal behavior:
-- Small updates (< 5% of file) -> MOR (fast write, compaction heals reads)
-- Large updates (>= 5% of file) -> COW (no read penalty at all)
SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' IN TABLE events_mor_demo;

-- Clean up
DROP ICEBERG TABLE IF EXISTS events_mor_demo;
ALTER SESSION UNSET QUERY_TAG;


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg with MOR:
    - Delete files accumulate indefinitely without manual maintenance
    - Read performance degrades linearly with each update/delete
    - Must schedule Spark "rewrite_data_files" to resolve
    - Trade-off: fast writes OR fast reads, pick one
  
  In Snowflake:
    - MOR gives fast writes via deletion vectors
    - Background compaction AUTOMATICALLY resolves deletion vectors
    - Read performance remains STABLE over time (self-healing)
    - AUTO mode picks the best strategy per-operation
    - No trade-off: get both fast writes AND fast reads
================================================================================
*/
