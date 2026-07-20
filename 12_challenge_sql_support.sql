/*
================================================================================
  CHALLENGE 11: INCONSISTENT SQL SUPPORT
================================================================================
  PROBLEM:
    Different query engines have varying levels of DML support on Iceberg:
    - Apache Spark: Full DML but requires specific configurations
    - Trino/Presto: Limited UPDATE/DELETE, no MERGE in older versions
    - Athena: Read-only or limited DML depending on version
    - Flink: Append-only in most configurations
    - Dremio: Partial DML support with restrictions
    
    This fragmentation means:
    - Teams write different code per engine
    - Some operations require switching engines mid-pipeline
    - CDC/MERGE patterns can't be implemented on some platforms
    - Testing must cover all engine combinations

  SNOWFLAKE MITIGATION:
    Snowflake supports FULL DML on Iceberg tables, identical to native tables:
    - INSERT (single row, bulk, SELECT INTO)
    - UPDATE (with arbitrary predicates)
    - DELETE (with arbitrary predicates)
    - MERGE (full CDC pattern with MATCHED/NOT MATCHED)
    - TRUNCATE TABLE
    - COPY INTO (bulk loading)
    - CREATE TABLE AS SELECT (CTAS)

  KEY INSIGHT:
    Iceberg tables in Snowflake behave IDENTICALLY to native Snowflake tables
    from a SQL perspective. No restrictions, no workarounds.

  PREREQUISITE: Run 00 and 01 first
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: INSERT operations (all variants)
-- ============================================

-- Single row INSERT
INSERT INTO customer_orders VALUES
  (20001, 'CUST_NEW_1', CURRENT_DATE(), NULL, 'PENDING', 299.99, 'us-east-1', 'HIGH', FALSE);

-- Multi-row INSERT
INSERT INTO customer_orders VALUES
  (20002, 'CUST_NEW_2', CURRENT_DATE(), NULL, 'PENDING', 149.50, 'eu-west-1', 'MEDIUM', FALSE),
  (20003, 'CUST_NEW_3', CURRENT_DATE(), NULL, 'PENDING', 599.00, 'ap-south-1', 'LOW', FALSE);

-- INSERT from SELECT (bulk)
INSERT INTO customer_orders
SELECT
    20000 + SEQ4() + 4,
    'CUST_BULK_' || SEQ4(),
    CURRENT_DATE(),
    NULL,
    'PENDING',
    ROUND(UNIFORM(50, 1000, RANDOM())::NUMBER(12,2), 2),
    'us-west-2',
    'MEDIUM',
    FALSE
FROM TABLE(GENERATOR(ROWCOUNT => 100));

SELECT COUNT(*) AS total_orders FROM customer_orders;


-- ============================================
-- STEP 2: UPDATE operations
-- ============================================

-- Simple UPDATE
UPDATE customer_orders
SET status = 'PROCESSING'
WHERE order_id = 20001;

-- Conditional UPDATE with multiple columns
UPDATE customer_orders
SET
    status = 'SHIPPED',
    ship_date = CURRENT_DATE()
WHERE status = 'PROCESSING'
  AND order_date < CURRENT_DATE() - 1
  AND order_id BETWEEN 20001 AND 20003;

-- UPDATE with subquery
UPDATE customer_orders t
SET t.priority = 'HIGH'
WHERE t.total_amount > (
    SELECT AVG(total_amount) * 2 FROM customer_orders
);

-- Verify updates
SELECT order_id, status, ship_date, priority
FROM customer_orders
WHERE order_id BETWEEN 20001 AND 20003;


-- ============================================
-- STEP 3: DELETE operations
-- ============================================

-- Simple DELETE
DELETE FROM customer_orders
WHERE order_id = 20003;

-- Conditional DELETE
DELETE FROM customer_orders
WHERE status = 'CANCELLED'
  AND order_date < CURRENT_DATE() - 180
  AND total_amount < 50;

-- DELETE with EXISTS (correlated)
DELETE FROM customer_orders o
WHERE EXISTS (
    SELECT 1 FROM orders_staging s
    WHERE s.order_id = o.order_id
      AND s.change_type = 'DELETE'
);

SELECT COUNT(*) AS remaining_orders FROM customer_orders;


-- ============================================
-- STEP 4: MERGE (Full CDC Pattern)
-- ============================================

-- This is the most powerful DML - and the one most engines struggle with.
-- Snowflake supports full MERGE on Iceberg tables:

-- Re-populate staging with fresh data for MERGE demo
TRUNCATE TABLE orders_staging;

INSERT INTO orders_staging
-- New orders
SELECT 30001 + SEQ4(), 'CUST_MERGE_' || SEQ4(), CURRENT_DATE(), NULL,
       'PENDING', ROUND(UNIFORM(100, 2000, RANDOM())::NUMBER(12,2), 2),
       'us-east-1', 'HIGH', FALSE, 'INSERT'
FROM TABLE(GENERATOR(ROWCOUNT => 50))
UNION ALL
-- Updates to existing orders
SELECT order_id, customer_id, order_date, CURRENT_DATE(), 'DELIVERED',
       total_amount, region, priority, is_confidential, 'UPDATE'
FROM customer_orders WHERE status = 'SHIPPED' SAMPLE (20 ROWS);

-- Execute MERGE (upsert pattern)
MERGE INTO customer_orders AS target
USING orders_staging AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.change_type = 'UPDATE' THEN
    UPDATE SET
        target.status = source.status,
        target.ship_date = source.ship_date
WHEN NOT MATCHED AND source.change_type = 'INSERT' THEN
    INSERT (order_id, customer_id, order_date, ship_date, status, total_amount, region, priority, is_confidential)
    VALUES (source.order_id, source.customer_id, source.order_date, source.ship_date, source.status,
            source.total_amount, source.region, source.priority, source.is_confidential);

-- Verify MERGE results
SELECT status, COUNT(*) AS cnt
FROM customer_orders
GROUP BY status
ORDER BY cnt DESC;


-- ============================================
-- STEP 5: TRUNCATE TABLE
-- ============================================

-- TRUNCATE works on Iceberg tables (removes all rows, keeps table structure)
CREATE OR REPLACE ICEBERG TABLE truncate_demo (id NUMBER(38,0), val STRING)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'iceberg_demo_vol'
  BASE_LOCATION = 'truncate_demo'
  TARGET_FILE_SIZE = 'AUTO';

INSERT INTO truncate_demo SELECT SEQ4(), 'value_' || SEQ4() FROM TABLE(GENERATOR(ROWCOUNT => 1000));
SELECT COUNT(*) FROM truncate_demo;  -- 1000

TRUNCATE TABLE truncate_demo;
SELECT COUNT(*) FROM truncate_demo;  -- 0


-- ============================================
-- STEP 6: CREATE TABLE AS SELECT (CTAS)
-- ============================================

-- Create a new Iceberg table from a query result
CREATE OR REPLACE ICEBERG TABLE high_value_orders
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'iceberg_demo_vol'
  BASE_LOCATION = 'high_value_orders'
  TARGET_FILE_SIZE = 'AUTO'
AS
SELECT *
FROM customer_orders
WHERE total_amount > 2000
  AND status IN ('SHIPPED', 'DELIVERED');

SELECT COUNT(*) AS high_value_count FROM high_value_orders;


-- ============================================
-- STEP 7: Verify - Full DML parity
-- ============================================

-- All operations succeeded on Iceberg tables without any restrictions
SELECT
    QUERY_TYPE,
    EXECUTION_STATUS,
    TOTAL_ELAPSED_TIME / 1000.0 AS elapsed_sec,
    ROWS_PRODUCED,
    ROWS_INSERTED,
    ROWS_UPDATED,
    ROWS_DELETED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE DATABASE_NAME = 'ICEBERG_CHALLENGES_DB'
  AND QUERY_TYPE IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE_TABLE_AS_SELECT')
ORDER BY START_TIME DESC
LIMIT 15;

-- Clean up demo tables
DROP ICEBERG TABLE IF EXISTS truncate_demo;
DROP ICEBERG TABLE IF EXISTS high_value_orders;


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg (inconsistent SQL support):
    - Spark: Full DML (with correct config), but MERGE syntax differs
    - Trino: UPDATE/DELETE supported, MERGE added only in recent versions
    - Athena: Limited DML, engine version dependent
    - Flink: Primarily append-only streaming
    - Presto: Read-mostly, limited write support
    - Teams must maintain different SQL for different engines
  
  In Snowflake (full SQL parity):
    - INSERT: All variants (single, bulk, SELECT INTO)
    - UPDATE: Arbitrary predicates, subqueries, correlated
    - DELETE: Arbitrary predicates, EXISTS, correlated
    - MERGE: Full MATCHED/NOT MATCHED with multiple clauses
    - TRUNCATE: Instant table clearing
    - CTAS: Create new Iceberg tables from queries
    - COPY INTO: Bulk loading from stages
    - IDENTICAL syntax to native Snowflake tables - zero learning curve
================================================================================
*/
