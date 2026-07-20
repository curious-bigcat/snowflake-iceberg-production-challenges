/*
================================================================================
  CHALLENGE 9: ORPHAN FILE ACCUMULATION
================================================================================
  PROBLEM:
    Failed writes, aborted transactions, and incomplete compaction jobs leave
    abandoned Parquet files on object storage that are NOT tracked by any
    metadata file. These "orphan files":
    - Silently consume storage ($$$)
    - Cannot be found by table queries (invisible to users)
    - Require complex scanning scripts to identify
    - Risk accidental deletion of valid files if cleanup is aggressive

  SNOWFLAKE MITIGATION:
    1. Automatic Snapshot Expiry - Snowflake deletes old snapshots and their
       unique data/metadata files based on DATA_RETENTION_TIME_IN_DAYS.
    2. Atomic transaction handling - Snowflake's write path minimizes orphans
       by using atomic metadata pointer updates.
    3. TABLE_STORAGE_METRICS - Detects storage/metadata mismatches.
    4. For any remaining orphans, Snowflake Support can assist with identification.

  KEY PARAMETERS:
    - DATA_RETENTION_TIME_IN_DAYS = 1-90 (Enterprise: up to 90)
    - Snapshot expiry: Always on, cannot be disabled, zero cost

  PREREQUISITE: Run 00 and 01 first
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Understand orphan file problem
-- ============================================

/*
  How orphan files are created in open-source Iceberg:
  
  1. Writer starts writing a Parquet file to S3
  2. Writer finishes the file upload
  3. Writer attempts to commit metadata (update metadata pointer)
  4. COMMIT FAILS (conflict, timeout, OOM, network error)
  5. The Parquet file exists on S3 but is NOT referenced in any metadata
  6. This file is now an "orphan" - consuming storage silently
  
  In production at scale, this happens hundreds of times per day.
  After a year, orphan files can represent 10-30% of total storage.
*/


-- ============================================
-- STEP 2: Snowflake's automatic lifecycle management
-- ============================================

-- Snapshot expiry is ALWAYS ON and ZERO COST for Snowflake-managed tables
-- It deletes old data files that are no longer referenced by any live snapshot

-- Check the retention period
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE sensor_readings;

-- Set retention to desired period (default: 1 day for Standard, configurable in Enterprise)
ALTER ICEBERG TABLE sensor_readings
  SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- After the retention period expires, Snowflake automatically:
-- 1. Identifies snapshots older than retention
-- 2. Finds data files unique to those expired snapshots
-- 3. Deletes those files from external storage
-- 4. Removes the expired metadata files

-- Verify setting
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE sensor_readings;


-- ============================================
-- STEP 3: Monitor storage for potential orphans
-- ============================================

-- Use TABLE_STORAGE_METRICS to check for mismatches between
-- S3 storage and Snowflake-tracked bytes
SELECT
    TABLE_NAME,
    ACTIVE_BYTES,
    TIME_TRAVEL_BYTES,
    RETAINED_FOR_CLONE_BYTES,
    IS_TRANSIENT
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_CATALOG = 'ICEBERG_CHALLENGES_DB'
  AND TABLE_SCHEMA = 'DEMO'
  AND ACTIVE_BYTES > 0
ORDER BY ACTIVE_BYTES DESC;

-- If your S3 bucket shows significantly MORE bytes than Snowflake reports,
-- you may have orphan files. Contact Snowflake Support for remediation.

-- Snowflake also provides the Information Schema version for near-real-time data:
SELECT
    TABLE_NAME,
    ACTIVE_BYTES,
    TIME_TRAVEL_BYTES
FROM ICEBERG_CHALLENGES_DB.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_SCHEMA = 'DEMO'
ORDER BY ACTIVE_BYTES DESC;


-- ============================================
-- STEP 4: Show that failed transactions don't leave orphans easily
-- ============================================

-- Snowflake's atomic commit mechanism minimizes orphan creation:
-- The metadata pointer update is atomic, so partial writes are less common.

-- Demonstrate: a failed transaction doesn't leave files behind
BEGIN TRANSACTION;
    INSERT INTO customer_orders
    SELECT
        99999, 'CUST_TEST', CURRENT_DATE(), NULL, 'PENDING',
        100.00, 'us-east-1', 'HIGH', FALSE;
ROLLBACK;

-- The rolled-back data does NOT persist as an orphan file
-- Snowflake handles the cleanup internally
SELECT COUNT(*) FROM customer_orders WHERE order_id = 99999;
-- Result: 0 (no orphan data)


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg (orphan file management):
    - Must run `removeOrphanFiles` Spark procedure regularly
    - Requires scanning ALL files on storage and comparing to metadata
    - Risk of accidentally deleting valid files if retention too aggressive
    - No built-in tooling (community scripts vary in reliability)
    - Orphans accumulate silently, discovered only via storage bill spikes
    - Example Spark command:
      spark.sql("CALL catalog.system.remove_orphan_files(
        table => 'db.table',
        older_than => timestamp '2024-01-01')")
  
  In Snowflake:
    - Automatic Snapshot Expiry handles file lifecycle (ZERO cost)
    - Atomic transactions minimize orphan creation
    - TABLE_STORAGE_METRICS for monitoring storage/metadata consistency
    - DATA_RETENTION_TIME_IN_DAYS controls cleanup timing
    - No external scanning scripts needed
    - Snowflake Support can assist with edge cases
================================================================================
*/
