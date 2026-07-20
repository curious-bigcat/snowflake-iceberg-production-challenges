/*
================================================================================
  CHALLENGE 2: METADATA FILE BLOATING
================================================================================
  PROBLEM:
    Every transaction generates JSON metadata files (manifest lists, manifests,
    and metadata.json). Over time, thousands of metadata files accumulate,
    slowing down manifest scans and table planning. In open-source Iceberg,
    you must manually run "rewrite manifests" maintenance.

  SNOWFLAKE MITIGATION:
    1. Automatic Manifest Compaction - Snowflake continuously reorganizes and
       combines smaller manifest files. This is ALWAYS ON and has ZERO COST.
    2. Automatic Snapshot Expiry - Old metadata files are deleted based on
       DATA_RETENTION_TIME_IN_DAYS.
    3. No user action required - Cannot be disabled, no scheduling needed.

  KEY FEATURES:
    - Manifest compaction: Always on, zero cost, cannot disable
    - Snapshot expiry: Always on, respects retention period
    - Metadata generated automatically on periodic basis

  PREREQUISITE: Run 00 and 01 first
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Understand metadata generation in Snowflake
-- ============================================

-- Snowflake generates Iceberg metadata automatically on a periodic basis.
-- Each new metadata file consolidates all DML/DDL changes since the last one.

-- View current metadata location
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_CHALLENGES_DB.DEMO.TRANSACTIONS');

-- In open-source Iceberg, every single commit creates a new:
--   metadata.json -> manifest-list -> manifest -> data files
-- This creates a tree that grows unboundedly.

-- Generate a fresh metadata snapshot on demand (optional - Snowflake does this automatically)
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_CHALLENGES_DB.DEMO.RISK_SCORES');


-- ============================================
-- STEP 2: Show Snowflake's automatic management
-- ============================================

-- Manifest compaction is BUILT-IN. Unlike Spark's `rewriteManifests()` action,
-- you never need to schedule or run it. Snowflake does it transparently.

-- The key insight: Snowflake batches changes into a SINGLE metadata file
-- rather than creating one per transaction. This fundamentally prevents bloat.

-- Show the retention period (controls when old metadata is cleaned up)
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE transactions;

-- The default retention is typically 1 day for Standard, up to 90 for Enterprise
-- Old snapshots and their metadata are automatically cleaned after this period


-- ============================================
-- STEP 3: Demonstrate - Many DMLs, one metadata file
-- ============================================

-- Perform multiple DML operations in quick succession
UPDATE risk_scores SET risk_category = 'HIGH' WHERE risk_category = 'LOW' AND assessment_id <= 100;
UPDATE risk_scores SET risk_category = 'CRITICAL' WHERE risk_category = 'HIGH' AND assessment_id <= 50;
DELETE FROM risk_scores WHERE risk_category = 'CRITICAL' AND assessment_id BETWEEN 1999900 AND 1999950;

-- Despite 3 DML operations, Snowflake consolidates metadata
-- In OSS Iceberg, this would create 3 separate metadata.json files

-- Get table info - notice it points to a SINGLE latest metadata file
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_CHALLENGES_DB.DEMO.RISK_SCORES');


-- ============================================
-- STEP 4: Verify - No metadata bloat accumulation
-- ============================================

-- Check that the optimization service handles manifest compaction
SELECT
    TABLE_NAME,
    START_TIME,
    END_TIME,
    CREDITS_USED,
    NUM_BYTES_SCANNED
FROM SNOWFLAKE.ACCOUNT_USAGE.ICEBERG_STORAGE_OPTIMIZATION_HISTORY
WHERE DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
ORDER BY START_TIME DESC
LIMIT 20;

-- For externally managed tables, you would need to run:
--   spark.sql("CALL catalog.system.rewrite_manifests('table')")
--   spark.sql("CALL catalog.system.expire_snapshots('table', ...)")
-- Snowflake eliminates this entirely for managed tables.


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg, metadata management requires:
    - Running Spark's `rewriteManifests()` procedure periodically
    - Scheduling `expireSnapshots()` jobs
    - Monitoring manifest file counts
    - Tuning `commit.manifest.target-size-bytes`
    - Handling failures in maintenance jobs
  
  In Snowflake:
    - Manifest compaction is AUTOMATIC, ALWAYS-ON, and FREE
    - Snapshot expiry is AUTOMATIC based on retention settings
    - Metadata is generated periodically (not per-commit), preventing bloat
    - SYSTEM$GET_ICEBERG_TABLE_INFORMATION for on-demand metadata generation
    - Zero maintenance scheduling required
================================================================================
*/
