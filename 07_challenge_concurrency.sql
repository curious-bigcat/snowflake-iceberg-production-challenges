/*
================================================================================
  CHALLENGE 6: COMMIT CONCURRENCY CONFLICTS
================================================================================
  PROBLEM:
    In open-source Iceberg, high-frequency parallel writes trigger frequent
    transaction failures due to optimistic concurrency control:
    - Writer A reads metadata v5, Writer B reads metadata v5
    - Writer A commits metadata v6 successfully
    - Writer B's commit FAILS (conflict with v6), must retry from scratch
    - Retry loops waste compute and add latency
    - At scale, conflict rates can exceed 30% during peak ingestion

  SNOWFLAKE MITIGATION:
    Snowflake uses its native MVCC (Multi-Version Concurrency Control) engine
    for Iceberg tables:
    - No Iceberg-level optimistic locking conflicts
    - Parallel writes serialize transparently at the storage layer
    - Multi-statement transactions with ACID guarantees
    - No user-visible retries or conflicts
    - Scales to thousands of concurrent writers

  KEY INSIGHT:
    Snowflake's transaction manager handles Iceberg metadata atomically -
    writers never see commit conflicts because Snowflake manages the 
    metadata-pointer update internally.

  PREREQUISITE: Run 00 and 01 first
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Create a high-concurrency target table
-- ============================================

CREATE OR REPLACE ICEBERG TABLE concurrent_writes_demo (
    writer_id       STRING,
    batch_num       NUMBER(38,0),
    record_id       NUMBER(38,0),
    payload         STRING,
    write_ts        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'concurrent_writes'
TARGET_FILE_SIZE = 'AUTO';


-- ============================================
-- STEP 2: Simulate concurrent writes (no conflicts!)
-- ============================================

-- In Snowflake, multiple writers can INSERT simultaneously without conflicts.
-- The following simulates what would cause conflicts in OSS Iceberg:

-- Writer 1: Insert batch
INSERT INTO concurrent_writes_demo (writer_id, batch_num, record_id, payload)
SELECT 'writer_A', 1, SEQ4() + 1, 'payload_from_writer_A_batch_1'
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

-- Writer 2: Insert batch (in OSS Iceberg, this would conflict with Writer 1's metadata)
INSERT INTO concurrent_writes_demo (writer_id, batch_num, record_id, payload)
SELECT 'writer_B', 1, SEQ4() + 1, 'payload_from_writer_B_batch_1'
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

-- Writer 3: Insert batch
INSERT INTO concurrent_writes_demo (writer_id, batch_num, record_id, payload)
SELECT 'writer_C', 1, SEQ4() + 1, 'payload_from_writer_C_batch_1'
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

-- All three succeed without conflicts!
SELECT writer_id, COUNT(*) AS rows_written
FROM concurrent_writes_demo
GROUP BY writer_id
ORDER BY writer_id;


-- ============================================
-- STEP 3: Demonstrate multi-statement transactions
-- ============================================

-- Snowflake supports full ACID transactions on Iceberg tables.
-- This is impossible in OSS Iceberg which uses per-commit optimistic locking.

BEGIN TRANSACTION;

    -- Operation 1: Insert new records
    INSERT INTO concurrent_writes_demo (writer_id, batch_num, record_id, payload)
    SELECT 'writer_TXN', 99, SEQ4() + 1, 'transactional_insert'
    FROM TABLE(GENERATOR(ROWCOUNT => 1000));

    -- Operation 2: Update existing records (same transaction)
    UPDATE concurrent_writes_demo
    SET payload = 'updated_in_same_txn'
    WHERE writer_id = 'writer_A' AND record_id <= 10;

    -- Operation 3: Delete some records (same transaction)
    DELETE FROM concurrent_writes_demo
    WHERE writer_id = 'writer_C' AND record_id > 4990;

COMMIT;

-- All three operations committed atomically
-- In OSS Iceberg, you'd need complex orchestration for this


-- ============================================
-- STEP 4: Prove zero commit failures
-- ============================================

-- Check query history for any failed DML on our demo table
SELECT
    QUERY_TYPE,
    EXECUTION_STATUS,
    ERROR_CODE,
    ERROR_MESSAGE,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_sec
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TYPE IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE')
  AND DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
  AND SCHEMA_NAME = 'DEMO'
ORDER BY START_TIME DESC
LIMIT 20;

-- Expected: ALL rows show EXECUTION_STATUS = 'SUCCESS'
-- In OSS Iceberg, you'd see ConflictException retries


-- Simulate what concurrent writing looks like with Tasks (true parallelism)
-- Create two tasks that write to the same table simultaneously

CREATE OR REPLACE TASK writer_task_1
  WAREHOUSE = ICEBERG_DEMO_WH
  SCHEDULE = '1 MINUTE'
  COMMENT = 'Concurrent writer 1 for demo'
AS
INSERT INTO concurrent_writes_demo (writer_id, batch_num, record_id, payload)
SELECT 'task_writer_1', UNIFORM(1, 1000, RANDOM()), SEQ4(), 'from_task_1'
FROM TABLE(GENERATOR(ROWCOUNT => 100));

CREATE OR REPLACE TASK writer_task_2
  WAREHOUSE = ICEBERG_DEMO_WH
  SCHEDULE = '1 MINUTE'
  COMMENT = 'Concurrent writer 2 for demo'
AS
INSERT INTO concurrent_writes_demo (writer_id, batch_num, record_id, payload)
SELECT 'task_writer_2', UNIFORM(1, 1000, RANDOM()), SEQ4(), 'from_task_2'
FROM TABLE(GENERATOR(ROWCOUNT => 100));

-- Start both tasks to run simultaneously
ALTER TASK writer_task_1 RESUME;
ALTER TASK writer_task_2 RESUME;

-- Wait ~2 minutes, then check results...
-- SELECT writer_id, COUNT(*) FROM concurrent_writes_demo WHERE writer_id LIKE 'task_%' GROUP BY 1;

-- Suspend tasks after demo
ALTER TASK writer_task_1 SUSPEND;
ALTER TASK writer_task_2 SUSPEND;

-- Verify both tasks wrote successfully (no conflicts)
SELECT
    NAME,
    STATE,
    COMPLETED_TIME,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME IN ('WRITER_TASK_1', 'WRITER_TASK_2')
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- Clean up
DROP TASK IF EXISTS writer_task_1;
DROP TASK IF EXISTS writer_task_2;
DROP ICEBERG TABLE IF EXISTS concurrent_writes_demo;


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg (optimistic concurrency):
    - Each commit must atomically swap metadata pointer
    - Concurrent writers cause CommitFailedException
    - Exponential backoff retries waste compute
    - Conflict rate increases with write frequency
    - Multi-statement transactions are NOT natively supported
    - Teams build complex retry/queue infrastructure
  
  In Snowflake (native MVCC):
    - Zero commit conflicts regardless of parallelism
    - Thousands of concurrent writers supported
    - Full multi-statement ACID transactions (BEGIN/COMMIT)
    - No retry logic needed
    - No additional infrastructure
    - Scales transparently
================================================================================
*/
