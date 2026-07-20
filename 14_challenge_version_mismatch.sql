/*
================================================================================
  CHALLENGE 13: FORMAT VERSION MISMATCHES
================================================================================
  PROBLEM:
    The Apache Iceberg specification evolves across versions (v1, v2, v3), and
    newer features often break compatibility with older query engines:
    - v2 introduced: position delete files, schema evolution, partition evolution
    - v3 introduced: deletion vectors, row lineage, default values
    - Older Spark/Trino versions cannot read v3 deletion vectors
    - Engines may silently return wrong results on unsupported features
    - Upgrading one engine can break compatibility with another
    - No central control over which version tables use
    - Teams discover incompatibilities only at query time (production failures)

  SNOWFLAKE MITIGATION:
    Snowflake provides centralized version control:
    1. ICEBERG_VERSION_DEFAULT - Account/database/schema level default
    2. ICEBERG_VERSION per table - Override for specific tables
    3. ICEBERG_MERGE_ON_READ_BEHAVIOR = 'DISABLED' - Force COW for v2 readers
    4. Controlled upgrade path - Change versions when all consumers are ready
    5. Both v2 and v3 tables can coexist in the same database

  KEY PARAMETERS:
    - ICEBERG_VERSION_DEFAULT (account/database/schema level)
    - ICEBERG_VERSION (table level, at CREATE time)
    - ICEBERG_MERGE_ON_READ_BEHAVIOR (controls deletion vector usage)

  PREREQUISITE: Run 00 and 01 first
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Show version control at multiple levels
-- ============================================

-- Check current database default
SHOW PARAMETERS LIKE 'ICEBERG_VERSION_DEFAULT' IN DATABASE ICEBERG_CHALLENGES_DB;

-- You can set version defaults at different scopes:
-- Account level (global default for all new Iceberg tables)
-- ALTER ACCOUNT SET ICEBERG_VERSION_DEFAULT = 3;

-- Database level (applies to all tables in this database)
ALTER DATABASE ICEBERG_CHALLENGES_DB SET ICEBERG_VERSION_DEFAULT = 3;

-- Schema level (for mixed environments)
-- CREATE SCHEMA legacy_apps;
-- ALTER SCHEMA legacy_apps SET ICEBERG_VERSION_DEFAULT = 2;

-- Verify the hierarchy
SHOW PARAMETERS LIKE 'ICEBERG_VERSION_DEFAULT' IN DATABASE ICEBERG_CHALLENGES_DB;


-- ============================================
-- STEP 2: Create tables at different Iceberg versions
-- ============================================

-- v2 table: Compatible with older Spark (3.3+), Trino (405+), Athena v3
CREATE OR REPLACE ICEBERG TABLE orders_v2 (
    order_id        NUMBER(38,0),
    customer_id     STRING,
    amount          NUMBER(12,2),
    status          STRING,
    order_date      DATE,
    region          STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'version_demo/orders_v2'
ICEBERG_VERSION = 2
TARGET_FILE_SIZE = 'AUTO';

-- v3 table: Enables deletion vectors, row lineage, default values
CREATE OR REPLACE ICEBERG TABLE orders_v3 (
    order_id        NUMBER(38,0),
    customer_id     STRING,
    amount          NUMBER(12,2),
    status          STRING DEFAULT 'PENDING',
    order_date      DATE,
    region          STRING,
    event_version   INT DEFAULT 1
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'version_demo/orders_v3'
ICEBERG_VERSION = 3
TARGET_FILE_SIZE = 'AUTO';

-- Load identical data into both
INSERT INTO orders_v2
SELECT SEQ4()+1, 'CUST_'||LPAD(UNIFORM(1,1000,RANDOM())::VARCHAR,4,'0'),
       ROUND(UNIFORM(100, 10000, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2),
       CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN 'PENDING' WHEN 2 THEN 'SHIPPED' WHEN 3 THEN 'DELIVERED' END,
       DATEADD(day, -UNIFORM(1,180,RANDOM()), CURRENT_DATE()),
       CASE UNIFORM(1,4,RANDOM()) WHEN 1 THEN 'AMERICAS' WHEN 2 THEN 'EMEA' WHEN 3 THEN 'APAC' WHEN 4 THEN 'LATAM' END
FROM TABLE(GENERATOR(ROWCOUNT => 100000));

INSERT INTO orders_v3 (order_id, customer_id, amount, status, order_date, region)
SELECT SEQ4()+1, 'CUST_'||LPAD(UNIFORM(1,1000,RANDOM())::VARCHAR,4,'0'),
       ROUND(UNIFORM(100, 10000, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2),
       CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN 'PENDING' WHEN 2 THEN 'SHIPPED' WHEN 3 THEN 'DELIVERED' END,
       DATEADD(day, -UNIFORM(1,180,RANDOM()), CURRENT_DATE()),
       CASE UNIFORM(1,4,RANDOM()) WHEN 1 THEN 'AMERICAS' WHEN 2 THEN 'EMEA' WHEN 3 THEN 'APAC' WHEN 4 THEN 'LATAM' END
FROM TABLE(GENERATOR(ROWCOUNT => 100000));


-- ============================================
-- STEP 3: Demonstrate v3-only features
-- ============================================

-- FEATURE 1: Default Values (v3 only)
-- The orders_v3 table has DEFAULT values - new rows auto-fill status and event_version
INSERT INTO orders_v3 (order_id, customer_id, amount, order_date, region)
VALUES (200001, 'CUST_NEW', 999.99, CURRENT_DATE(), 'EMEA');

-- status defaults to 'PENDING', event_version defaults to 1
SELECT * FROM orders_v3 WHERE order_id = 200001;

-- Change the write default for future writes
ALTER ICEBERG TABLE orders_v3 ALTER COLUMN event_version SET WRITE DEFAULT 2;

INSERT INTO orders_v3 (order_id, customer_id, amount, order_date, region)
VALUES (200002, 'CUST_NEW2', 1500.00, CURRENT_DATE(), 'APAC');

-- event_version is now 2 for new rows
SELECT * FROM orders_v3 WHERE order_id >= 200001 ORDER BY order_id;


-- FEATURE 2: Deletion Vectors (v3) vs Position Deletes (v2)
-- v3: Uses efficient deletion vectors
ALTER ICEBERG TABLE orders_v3 SET ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ENABLED';

ALTER SESSION SET QUERY_TAG = 'VERSION_V3_UPDATE';
UPDATE orders_v3 SET status = 'CANCELLED' WHERE order_id BETWEEN 1 AND 100;

-- v2: Uses copy-on-write (or position deletes if MOR forced)
ALTER ICEBERG TABLE orders_v2 SET ICEBERG_MERGE_ON_READ_BEHAVIOR = 'DISABLED';

ALTER SESSION SET QUERY_TAG = 'VERSION_V2_UPDATE';
UPDATE orders_v2 SET status = 'CANCELLED' WHERE order_id BETWEEN 1 AND 100;

-- Compare UPDATE performance (v3 should be faster due to deletion vectors)
SELECT
    QUERY_TAG,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_seconds,
    ROWS_UPDATED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG IN ('VERSION_V3_UPDATE', 'VERSION_V2_UPDATE')
  AND QUERY_TYPE = 'UPDATE'
ORDER BY QUERY_TAG;


-- ============================================
-- STEP 4: Managing compatibility for external readers
-- ============================================

-- If external Spark (< 3.5) needs to read your table, force COW to avoid
-- writing deletion vectors that old Spark can't read:
ALTER ICEBERG TABLE orders_v3
  SET ICEBERG_MERGE_ON_READ_BEHAVIOR = 'DISABLED';

-- Now updates will use Copy-on-Write (compatible with all readers)
UPDATE orders_v3 SET status = 'SHIPPED' WHERE order_id BETWEEN 101 AND 200;

-- When you've verified all consumers support v3, re-enable MOR:
-- ALTER ICEBERG TABLE orders_v3 SET ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ENABLED';

-- Generate metadata for external consumers to verify format
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_CHALLENGES_DB.DEMO.ORDERS_V2') AS v2_metadata;
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_CHALLENGES_DB.DEMO.ORDERS_V3') AS v3_metadata;


-- ============================================
-- STEP 5: Version migration strategy
-- ============================================

/*
  RECOMMENDED MIGRATION PATH:
  
  Phase 1: Set database default to v3 for new tables
    ALTER DATABASE my_db SET ICEBERG_VERSION_DEFAULT = 3;
  
  Phase 2: Keep existing v2 tables as-is (they continue working)
    -- No action needed, v2 tables remain on v2
  
  Phase 3: For tables needing v3 features, recreate:
    CREATE ICEBERG TABLE new_table_v3 ... ICEBERG_VERSION = 3 AS SELECT * FROM old_table_v2;
    -- Then swap: ALTER TABLE old_table_v2 SWAP WITH new_table_v3;
  
  Phase 4: Control MOR behavior per-table based on consumer compatibility:
    -- Tables read by Spark 3.3: ICEBERG_MERGE_ON_READ_BEHAVIOR = 'DISABLED'
    -- Tables read only by Snowflake: ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ENABLED'
*/

-- Show both versions coexisting in the same schema
DESCRIBE ICEBERG TABLE orders_v2;
DESCRIBE ICEBERG TABLE orders_v3;

-- Verify Iceberg version via metadata
SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' IN TABLE orders_v2;
SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' IN TABLE orders_v3;


-- ============================================
-- STEP 6: Compatibility matrix reference
-- ============================================

/*
  ICEBERG VERSION COMPATIBILITY MATRIX:

  ┌──────────────────────┬─────────┬─────────┬─────────────────────────────────────┐
  │ Engine               │ v1      │ v2      │ v3                                  │
  ├──────────────────────┼─────────┼─────────┼─────────────────────────────────────┤
  │ Snowflake            │ Read    │ Full    │ Full (deletion vectors, defaults,   │
  │                      │         │         │ row lineage)                        │
  │ Spark 3.3+           │ Full    │ Full    │ Read (no deletion vector writes)    │
  │ Spark 3.5+           │ Full    │ Full    │ Full                                │
  │ Trino 405+           │ Full    │ Full    │ Partial (depends on version)        │
  │ Athena v3            │ Read    │ Read/DML│ Limited                             │
  │ Flink 1.16+          │ Full    │ Full    │ Partial                             │
  │ Dremio               │ Full    │ Full    │ Partial                             │
  │ StarRocks            │ Read    │ Read    │ Limited                             │
  └──────────────────────┴─────────┴─────────┴─────────────────────────────────────┘
  
  Snowflake gives you CENTRAL CONTROL over which version each table uses,
  allowing safe coexistence and controlled upgrades.
*/

-- Clean up
DROP ICEBERG TABLE IF EXISTS orders_v2;
DROP ICEBERG TABLE IF EXISTS orders_v3;
ALTER SESSION UNSET QUERY_TAG;


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg (version mismatch chaos):
    - No central control over table format versions
    - Each engine may create tables at different spec versions
    - Upgrading one engine can break reads from another
    - Deletion vectors (v3) invisible to older engines = wrong results
    - Schema evolution behavior differs between v1/v2/v3
    - Discovery of incompatibility only at query time (production failure)
    - No easy way to check which version a table uses across platforms
  
  In Snowflake (centralized version governance):
    - ICEBERG_VERSION_DEFAULT at account/database/schema level
    - Per-table ICEBERG_VERSION override at creation time
    - v2 and v3 tables coexist safely in same database
    - ICEBERG_MERGE_ON_READ_BEHAVIOR controls compatibility per-table
    - Controlled migration path: set default, migrate incrementally
    - DESCRIBE ICEBERG TABLE shows version and features
    - SYSTEM$GET_ICEBERG_TABLE_INFORMATION for external consumer verification
================================================================================
*/
