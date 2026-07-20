/*
================================================================================
  CHALLENGE 7: CATALOG SYNCHRONIZATION DRIFT
================================================================================
  PROBLEM:
    When Iceberg tables are managed by an external catalog (Glue, Unity Catalog,
    Polaris), keeping metadata synced across multiple platforms is error-prone:
    - External writes update the catalog but Snowflake has stale metadata
    - Manual REFRESH commands are needed after every external change
    - Missed refreshes lead to queries returning outdated data
    - Multiple consumers reading different snapshots causes inconsistency
    - Event notification setup is complex (SNS/SQS for AWS, EventGrid for Azure)

  SNOWFLAKE MITIGATION:
    Option A (Snowflake-managed catalog): No sync needed - Snowflake IS the catalog
    Option B (External catalog): AUTO_REFRESH with event notifications
    - Automatic metadata refresh when external changes are detected
    - Event-driven (SNS/SQS) or polling-based refresh
    - Catalog-Linked Databases (CLD) for zero-config multi-table sync

  KEY FEATURES:
    - CATALOG = 'SNOWFLAKE' -> zero drift (Snowflake manages everything)
    - AUTO_REFRESH = TRUE -> event-driven sync for external catalogs
    - Catalog-Linked Databases -> auto-discover and sync all tables

  PREREQUISITE: Run 00 and 01 first
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Show how Snowflake-managed tables eliminate drift entirely
-- ============================================

-- When Snowflake IS the catalog, there is no synchronization needed.
-- All writers go through Snowflake -> metadata is always consistent.

-- Our sensor_readings table uses CATALOG = 'SNOWFLAKE'
-- Any read always sees the latest committed data.
SELECT COUNT(*) AS current_row_count FROM sensor_readings;

-- Insert data and it's immediately visible (no refresh needed)
INSERT INTO sensor_readings (device_id, sensor_type, reading_value, event_ts, region, quality_flag)
VALUES ('device_sync_test', 'temperature', 42.5, CURRENT_TIMESTAMP(), 'us-east-1', 1);

-- Immediately queryable - no catalog sync delay
SELECT * FROM sensor_readings WHERE device_id = 'device_sync_test';

-- Generate Iceberg metadata for external consumers
-- This makes the table readable by Spark/Trino/etc.
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_CHALLENGES_DB.DEMO.SENSOR_READINGS');


-- ============================================
-- STEP 2: Show AUTO_REFRESH for externally managed tables
-- ============================================

-- For tables managed by an external catalog (e.g., Glue, Unity Catalog),
-- Snowflake supports automatic refresh via event notifications.
-- Below is the PATTERN (requires an actual external catalog integration):

/*
-- Example: Create an externally managed table with AUTO_REFRESH
CREATE OR REPLACE ICEBERG TABLE external_events
  CATALOG = my_glue_catalog_integration     -- Your catalog integration
  EXTERNAL_VOLUME = iceberg_demo_vol
  AUTO_REFRESH = TRUE                       -- Automatically syncs on changes
  CATALOG_TABLE_NAME = 'my_glue_database.events'
  CATALOG_NAMESPACE = 'production';

-- Snowflake will automatically:
-- 1. Listen for SNS/SQS notifications when Glue metadata changes
-- 2. Refresh the Iceberg metadata pointer
-- 3. Make new data visible without manual intervention
-- 4. Track refresh history for auditing

-- Monitor refresh status:
SELECT *
FROM TABLE(INFORMATION_SCHEMA.ICEBERG_TABLE_SNAPSHOT_REFRESH_HISTORY(
    TABLE_NAME => 'external_events'
))
ORDER BY REFRESH_START_TIME DESC;
*/


-- ============================================
-- STEP 3: Demonstrate manual refresh (for comparison)
-- ============================================

-- For externally managed tables WITHOUT auto-refresh,
-- you would need to manually refresh:
-- ALTER ICEBERG TABLE external_table REFRESH;
-- ALTER ICEBERG TABLE external_table REFRESH 'metadata/v2.metadata.json';

-- With Snowflake-managed tables, this is NEVER needed because
-- Snowflake IS the source of truth for metadata.


-- ============================================
-- STEP 4: Show Catalog-Linked Database concept
-- ============================================

-- Catalog-Linked Databases (CLD) are the ultimate solution:
-- Connect an external catalog ONCE, and ALL tables sync automatically.

/*
-- Example: Create a Catalog-Linked Database
CREATE DATABASE my_glue_lakehouse
  LINKED_CATALOG = my_glue_catalog_integration  -- Your catalog integration
  AUTO_REFRESH = TRUE;                          -- All tables auto-refresh

-- Now ALL tables in the Glue database appear as schemas/tables in Snowflake
-- New tables added to Glue are automatically discovered
-- Schema changes in Glue are automatically reflected
-- Zero manual intervention for multi-table sync

-- Verify synced tables:
SHOW ICEBERG TABLES IN DATABASE my_glue_lakehouse;
*/


-- ============================================
-- STEP 5: Key comparison
-- ============================================

/*
  CATALOG SYNC COMPARISON:

  +------------------------------+---------------------------+--------------------------------+
  | Scenario                     | OSS Iceberg               | Snowflake                      |
  +------------------------------+---------------------------+--------------------------------+
  | Snowflake-managed table      | N/A                       | Zero drift (Snowflake is       |
  |                              |                           | the single source of truth)    |
  +------------------------------+---------------------------+--------------------------------+
  | External catalog (per-table) | Manual REFRESH after each | AUTO_REFRESH = TRUE with       |
  |                              | external write            | event-driven notifications     |
  +------------------------------+---------------------------+--------------------------------+
  | External catalog (all tables)| Maintain sync scripts per | Catalog-Linked Database:       |
  |                              | table, handle schema      | auto-discover all tables,      |
  |                              | evolution manually        | auto-sync changes              |
  +------------------------------+---------------------------+--------------------------------+
  | Multi-platform consistency   | Complex CDC + polling     | Single metadata pointer        |
  |                              | infrastructure            | managed by Snowflake           |
  +------------------------------+---------------------------+--------------------------------+
*/

-- Clean up test data
DELETE FROM sensor_readings WHERE device_id = 'device_sync_test';


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg multi-platform setups:
    - Each consumer must independently track the latest metadata pointer
    - Manual refresh scripts per table per platform
    - Complex event notification infrastructure (SNS/SQS/EventGrid)
    - Schema drift between platforms goes undetected
    - New tables require manual registration everywhere
  
  In Snowflake:
    - CATALOG = 'SNOWFLAKE': Zero drift, single source of truth
    - AUTO_REFRESH = TRUE: Event-driven automatic sync for external catalogs
    - Catalog-Linked Database: Auto-discover and sync ALL tables
    - SYSTEM$GET_ICEBERG_TABLE_INFORMATION: On-demand metadata for external readers
    - Refresh history tracked for auditing and debugging
================================================================================
*/
