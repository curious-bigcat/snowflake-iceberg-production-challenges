/*
================================================================================
  CHALLENGE 10: MANUAL STORAGE CLEANUP
================================================================================
  PROBLEM:
    Engineers must explicitly schedule and manage multiple maintenance operations:
    - Snapshot expiration (expireSnapshots)
    - Orphan file removal (removeOrphanFiles)
    - Metadata file cleanup (rewriteManifests)
    - Delete file compaction (rewriteDataFiles)
    
    This requires:
    - Dedicated Spark/Flink clusters for maintenance
    - Airflow/Dagster/Prefect DAGs for scheduling
    - Monitoring to ensure jobs don't fail silently
    - Tuning retention windows to balance cost vs time-travel
    - On-call engineers for failed maintenance jobs

  SNOWFLAKE MITIGATION:
    ALL storage cleanup is FULLY AUTOMATIC in Snowflake:
    - Snapshot expiry: Automatic, always on, zero cost
    - Data compaction: Automatic, serverless, enabled by default
    - Manifest compaction: Automatic, always on, zero cost
    - Orphan prevention: Atomic transactions minimize orphan creation
    
    ZERO scheduling, ZERO infrastructure, ZERO manual intervention.

  KEY INSIGHT:
    Snowflake turns 4 separate maintenance operations into ZERO operations
    for the user.

  PREREQUISITE: Run 00 and 01 first
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Show what OSS Iceberg maintenance looks like
-- ============================================

/*
  In open-source Iceberg, a typical Airflow DAG for table maintenance:
  
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  @daily maintenance DAG (per table!)                                     │
  ├─────────────────────────────────────────────────────────────────────────┤
  │                                                                         │
  │  Task 1: expire_snapshots                                              │
  │    spark.sql("CALL catalog.system.expire_snapshots(                    │
  │      table => 'db.table',                                              │
  │      older_than => timestamp '...',                                    │
  │      retain_last => 5)")                                               │
  │                                                                         │
  │  Task 2: remove_orphan_files                                           │
  │    spark.sql("CALL catalog.system.remove_orphan_files(                 │
  │      table => 'db.table',                                              │
  │      older_than => timestamp '...')")                                  │
  │                                                                         │
  │  Task 3: rewrite_manifests                                             │
  │    spark.sql("CALL catalog.system.rewrite_manifests(                   │
  │      table => 'db.table')")                                            │
  │                                                                         │
  │  Task 4: rewrite_data_files (compaction)                               │
  │    spark.sql("CALL catalog.system.rewrite_data_files(                  │
  │      table => 'db.table',                                              │
  │      strategy => 'binpack',                                            │
  │      options => map('target-file-size-bytes', '134217728'))")          │
  │                                                                         │
  └─────────────────────────────────────────────────────────────────────────┘
  
  Multiply this by EVERY table in your lakehouse = massive operational burden.
*/


-- ============================================
-- STEP 2: Show Snowflake's zero-ops maintenance status
-- ============================================

-- All 4 operations are handled automatically. Let's verify:

-- 1. SNAPSHOT EXPIRY: Always on, controlled by retention
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE sensor_readings;

-- 2. DATA COMPACTION: Enabled by default
SHOW PARAMETERS LIKE 'ENABLE_DATA_COMPACTION' IN TABLE sensor_readings;

-- 3. MANIFEST COMPACTION: Always on (cannot be disabled, zero cost)
-- No parameter to show - it's unconditionally active

-- 4. TARGET FILE SIZE: Controls compaction target
SHOW PARAMETERS LIKE 'TARGET_FILE_SIZE' IN TABLE sensor_readings;

-- Summary: ALL maintenance is automatic. No DAGs, no scheduling, no Spark clusters.


-- ============================================
-- STEP 3: Monitor automatic maintenance (observability)
-- ============================================

-- Even though maintenance is automatic, you have full visibility:

-- View compaction and optimization activity
SELECT
    TABLE_NAME,
    START_TIME,
    END_TIME,
    DATEDIFF('second', START_TIME, END_TIME) AS duration_sec,
    CREDITS_USED,
    NUM_BYTES_SCANNED / (1024*1024) AS mb_scanned,
    NUM_ROWS_WRITTEN
FROM SNOWFLAKE.ACCOUNT_USAGE.ICEBERG_STORAGE_OPTIMIZATION_HISTORY
WHERE DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
ORDER BY START_TIME DESC
LIMIT 20;

-- Total maintenance cost across all tables
SELECT
    TABLE_NAME,
    COUNT(*) AS maintenance_runs,
    SUM(CREDITS_USED) AS total_credits,
    SUM(NUM_BYTES_SCANNED) / (1024*1024*1024) AS total_gb_processed
FROM SNOWFLAKE.ACCOUNT_USAGE.ICEBERG_STORAGE_OPTIMIZATION_HISTORY
WHERE DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
GROUP BY TABLE_NAME
ORDER BY total_credits DESC;


-- ============================================
-- STEP 4: Fine-tune retention if needed (optional)
-- ============================================

-- You CAN adjust retention per table based on compliance/cost needs:

-- Short retention for high-volume, low-value data
ALTER ICEBERG TABLE sensor_readings SET DATA_RETENTION_TIME_IN_DAYS = 1;

-- Longer retention for business-critical data
ALTER ICEBERG TABLE customer_orders SET DATA_RETENTION_TIME_IN_DAYS = 14;

-- Verify
SELECT 'sensor_readings' AS tbl, *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE sensor_readings;
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE customer_orders;

-- You can also disable compaction for specific tables (e.g., cold storage)
-- ALTER ICEBERG TABLE archive_table SET ENABLE_DATA_COMPACTION = FALSE;


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg (manual maintenance):
    - 4 separate maintenance procedures per table
    - Dedicated Spark clusters for maintenance ($$$)
    - Scheduling infrastructure (Airflow/Dagster/cron)
    - Per-table tuning of retention windows
    - Monitoring for failed maintenance jobs
    - On-call rotation for maintenance failures
    - Multiply by hundreds of tables = full-time engineering effort
  
  In Snowflake (fully automatic):
    - Snapshot expiry: AUTOMATIC, ALWAYS ON, ZERO COST
    - Data compaction: AUTOMATIC, SERVERLESS, enabled by default
    - Manifest compaction: AUTOMATIC, ALWAYS ON, ZERO COST  
    - Orphan prevention: Built into atomic transaction model
    - Full observability via ICEBERG_STORAGE_OPTIMIZATION_HISTORY
    - Optional: tune retention or disable compaction per-table
    - NET RESULT: Zero maintenance DAGs, zero Spark clusters, zero on-call
================================================================================
*/
