/*
================================================================================
  CHALLENGE 3: COMPACTION COMPUTE COSTS
================================================================================
  PROBLEM:
    Teams must run resource-heavy Spark/Flink optimization jobs to merge
    fragmented data files. This requires:
    - Dedicated Spark clusters (EMR/Databricks) running compaction jobs
    - Scheduling infrastructure (Airflow/Step Functions)
    - Tuning parallelism, memory, and bin-pack strategies
    - Paying for compute even when no compaction is needed
    Total cost: often 20-40% of the overall Iceberg compute budget.

  SNOWFLAKE MITIGATION:
    Snowflake performs data compaction as a SERVERLESS service:
    - No cluster provisioning
    - Pay only for actual compaction work (per-credit billing)
    - Automatic scaling based on workload
    - Transparent cost tracking via ICEBERG_STORAGE_OPTIMIZATION_HISTORY

  KEY MONITORING:
    - SNOWFLAKE.ACCOUNT_USAGE.ICEBERG_STORAGE_OPTIMIZATION_HISTORY
    - Credits billed only when compaction runs (often negligible)
    - Can disable per-table if not needed (ENABLE_DATA_COMPACTION = FALSE)

  PREREQUISITE: Run 00 and 01 first
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Show what compaction costs look like in Snowflake
-- ============================================

-- Query all compaction activity in this account
-- Note: CREDITS_USED is typically very small (fractions of a credit)
SELECT
    TABLE_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    START_TIME,
    END_TIME,
    DATEDIFF('second', START_TIME, END_TIME) AS duration_seconds,
    CREDITS_USED,
    NUM_BYTES_SCANNED,
    NUM_ROWS_WRITTEN
FROM SNOWFLAKE.ACCOUNT_USAGE.ICEBERG_STORAGE_OPTIMIZATION_HISTORY
WHERE DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
ORDER BY START_TIME DESC
LIMIT 20;

-- Aggregate: total compaction cost for our demo tables
SELECT
    TABLE_NAME,
    COUNT(*) AS compaction_runs,
    SUM(CREDITS_USED) AS total_credits,
    SUM(NUM_BYTES_SCANNED) / (1024*1024) AS total_mb_scanned,
    SUM(NUM_ROWS_WRITTEN) AS total_rows_compacted
FROM SNOWFLAKE.ACCOUNT_USAGE.ICEBERG_STORAGE_OPTIMIZATION_HISTORY
WHERE DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
GROUP BY TABLE_NAME
ORDER BY total_credits DESC;


-- ============================================
-- STEP 2: Compare with OSS Iceberg compaction costs
-- ============================================

/*
  COST COMPARISON (typical production scenario):

  +-------------------------------+---------------------+----------------------------+
  | Aspect                        | OSS Iceberg + Spark | Snowflake Managed Iceberg  |
  +-------------------------------+---------------------+----------------------------+
  | Infrastructure                | EMR/Databricks      | None (serverless)          |
  | Minimum cluster cost          | ~$0.50/hr minimum   | $0 when idle               |
  | Scheduling                    | Airflow/cron        | Automatic                  |
  | Bin-pack strategy tuning      | Manual              | Automatic                  |
  | Failed job handling           | Manual retry logic  | Automatic                  |
  | Cost for 1TB table/day        | ~$5-15/day          | ~$0.01-0.50/day            |
  | Monitoring                    | Custom dashboards   | Built-in view              |
  +-------------------------------+---------------------+----------------------------+
*/


-- ============================================
-- STEP 3: Demonstrate cost control
-- ============================================

-- You CAN disable compaction for tables that don't need it
-- (e.g., archival tables that are rarely queried)
CREATE OR REPLACE ICEBERG TABLE archive_logs (
    log_id NUMBER,
    log_message VARCHAR,
    created_at TIMESTAMP_NTZ
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'archive_logs'
ENABLE_DATA_COMPACTION = FALSE;  -- Disable for cost savings on cold tables

-- Verify compaction is disabled
SHOW PARAMETERS LIKE 'ENABLE_DATA_COMPACTION' IN TABLE archive_logs;

-- Re-enable when needed (e.g., before a large analytical query)
ALTER ICEBERG TABLE archive_logs SET ENABLE_DATA_COMPACTION = TRUE;

-- You can also control at the database or schema level
-- ALTER DATABASE ICEBERG_CHALLENGES_DB SET ENABLE_DATA_COMPACTION = TRUE;
-- ALTER SCHEMA DEMO SET ENABLE_DATA_COMPACTION = FALSE;


-- ============================================
-- STEP 4: Verify - Track cost over time
-- ============================================

-- Daily compaction cost trend (useful for budgeting)
SELECT
    DATE_TRUNC('day', START_TIME) AS compaction_date,
    COUNT(*) AS jobs_run,
    SUM(CREDITS_USED) AS daily_credits,
    SUM(NUM_BYTES_SCANNED) / (1024*1024*1024) AS gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.ICEBERG_STORAGE_OPTIMIZATION_HISTORY
WHERE DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
  AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;

-- Clean up demo table
DROP ICEBERG TABLE IF EXISTS archive_logs;


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg, compaction requires:
    - Dedicated Spark/Flink clusters ($$$)
    - Job scheduling and orchestration
    - Manual bin-pack strategy configuration
    - Retry logic for failed compaction jobs
    - Separate monitoring infrastructure
  
  In Snowflake:
    - Compaction is SERVERLESS (no clusters to manage)
    - Pay only for actual work done (often < $1/day for moderate tables)
    - Transparent cost tracking in ICEBERG_STORAGE_OPTIMIZATION_HISTORY
    - Fine-grained control: enable/disable per table, schema, or database
    - No scheduling, no orchestration, no failure handling
================================================================================
*/
