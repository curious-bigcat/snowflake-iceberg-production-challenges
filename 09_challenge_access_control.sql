/*
================================================================================
  CHALLENGE 8: FRAGMENTED ACCESS CONTROL
================================================================================
  PROBLEM:
    Managing row and column-level security on Iceberg tables in open-source
    requires complex third-party tools:
    - No native row-level security in Iceberg format spec
    - Column masking requires external policy engines (Apache Ranger, etc.)
    - Different engines enforce different security models
    - Policies don't travel with the data
    - Auditing access across multiple engines is fragmented

  SNOWFLAKE MITIGATION:
    Native Row Access Policies and Dynamic Data Masking work directly on
    Iceberg tables, identical to native Snowflake tables:
    - Row Access Policies: Filter rows based on user role/context
    - Column Masking Policies: Mask/redact sensitive columns per role
    - Policies are enforced at the platform level (cannot be bypassed)
    - Single governance model regardless of table format (native or Iceberg)

  KEY FEATURES:
    - CREATE ROW ACCESS POLICY ... applied to Iceberg tables
    - CREATE MASKING POLICY ... applied to Iceberg columns
    - Role-based enforcement
    - Works with Snowflake-managed AND externally managed Iceberg tables

  PREREQUISITE: Run 00 and 01 first (roles created in 00)
================================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;
USE WAREHOUSE ICEBERG_DEMO_WH;


-- ============================================
-- STEP 1: Create a sensitive Iceberg table
-- ============================================

-- Create a table with sensitive data (PII, financial info)
CREATE OR REPLACE ICEBERG TABLE employee_compensation (
    employee_id     NUMBER(38,0),
    full_name       STRING,
    email           STRING,
    department      STRING,
    region          STRING,
    salary          NUMBER(12,2),
    ssn             STRING,
    performance     STRING,
    hire_date       DATE
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_demo_vol'
BASE_LOCATION = 'employee_compensation'
TARGET_FILE_SIZE = 'AUTO';

-- Load sample sensitive data
INSERT INTO employee_compensation
SELECT
    SEQ4() + 1,
    'Employee_' || SEQ4() + 1,
    'emp' || (SEQ4() + 1) || '@company.com',
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'Engineering' WHEN 2 THEN 'Sales'
        WHEN 3 THEN 'Marketing' WHEN 4 THEN 'Finance' WHEN 5 THEN 'HR'
    END,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'us-east-1' WHEN 2 THEN 'us-west-2'
        WHEN 3 THEN 'eu-west-1' WHEN 4 THEN 'ap-south-1'
    END,
    ROUND(UNIFORM(50000, 250000, RANDOM()) + RANDOM() / 1e12, 2),
    LPAD(UNIFORM(100, 999, RANDOM())::VARCHAR, 3, '0') || '-' ||
      LPAD(UNIFORM(10, 99, RANDOM())::VARCHAR, 2, '0') || '-' ||
      LPAD(UNIFORM(1000, 9999, RANDOM())::VARCHAR, 4, '0'),
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'EXCEEDS' WHEN 2 THEN 'MEETS'
        WHEN 3 THEN 'DEVELOPING' WHEN 4 THEN 'NEEDS_IMPROVEMENT'
    END,
    DATEADD(day, -UNIFORM(30, 3650, RANDOM()), CURRENT_DATE())
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- Grant SELECT to both roles
GRANT SELECT ON TABLE employee_compensation TO ROLE iceberg_analyst_role;
GRANT SELECT ON TABLE employee_compensation TO ROLE iceberg_engineer_role;


-- ============================================
-- STEP 2: Create Row Access Policy on Iceberg table
-- ============================================

-- Row Access Policy: Analysts can only see employees in their region
-- Engineers (and accountadmin) can see all rows
CREATE OR REPLACE ROW ACCESS POLICY region_filter_policy
AS (region_val VARCHAR) RETURNS BOOLEAN ->
  CASE
    -- Full access for engineers and admins
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ICEBERG_ENGINEER_ROLE') THEN TRUE
    -- Analysts can only see us-east-1 data (simulating regional restriction)
    WHEN CURRENT_ROLE() = 'ICEBERG_ANALYST_ROLE' THEN region_val = 'us-east-1'
    ELSE FALSE
  END;

-- Apply Row Access Policy to the Iceberg table
ALTER ICEBERG TABLE employee_compensation
  ADD ROW ACCESS POLICY region_filter_policy ON (region);


-- ============================================
-- STEP 3: Create Column Masking Policy on Iceberg table
-- ============================================

-- Masking Policy: SSN is fully masked for analysts, visible for engineers
CREATE OR REPLACE MASKING POLICY ssn_mask
AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ICEBERG_ENGINEER_ROLE') THEN val
    ELSE '***-**-' || RIGHT(val, 4)  -- Show only last 4 digits
  END;

-- Masking Policy: Salary is hidden for analysts
CREATE OR REPLACE MASKING POLICY salary_mask
AS (val NUMBER(12,2)) RETURNS NUMBER(12,2) ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ICEBERG_ENGINEER_ROLE') THEN val
    ELSE NULL  -- Analysts cannot see salary
  END;

-- Apply masking policies to Iceberg table columns
ALTER ICEBERG TABLE employee_compensation
  MODIFY COLUMN ssn SET MASKING POLICY ssn_mask;

ALTER ICEBERG TABLE employee_compensation
  MODIFY COLUMN salary SET MASKING POLICY salary_mask;


-- ============================================
-- STEP 4: Test access control enforcement
-- ============================================

-- Test as ACCOUNTADMIN (full access)
USE ROLE ACCOUNTADMIN;
SELECT employee_id, full_name, region, salary, ssn, department
FROM employee_compensation
LIMIT 10;
-- Result: All rows visible, salary and SSN in clear text

-- Test as Engineer (full access)
USE ROLE iceberg_engineer_role;
SELECT employee_id, full_name, region, salary, ssn, department
FROM employee_compensation
LIMIT 10;
-- Result: All rows visible, salary and SSN in clear text

-- Test as Analyst (restricted)
USE ROLE iceberg_analyst_role;
SELECT employee_id, full_name, region, salary, ssn, department
FROM employee_compensation
LIMIT 10;
-- Result: ONLY us-east-1 rows, salary is NULL, SSN shows ***-**-XXXX

-- Count to prove row filtering
SELECT COUNT(*) AS visible_rows, region
FROM employee_compensation
GROUP BY region;
-- Analyst sees only us-east-1 rows

-- Switch back
USE ROLE ACCOUNTADMIN;


-- ============================================
-- STEP 5: Verify policy enforcement
-- ============================================

-- Show policies attached to the table
SELECT *
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    REF_ENTITY_NAME => 'ICEBERG_CHALLENGES_DB.DEMO.EMPLOYEE_COMPENSATION',
    REF_ENTITY_DOMAIN => 'TABLE'
));

-- Verify the policies are listed
SHOW ROW ACCESS POLICIES IN SCHEMA DEMO;
SHOW MASKING POLICIES IN SCHEMA DEMO;


/*
================================================================================
  KEY TAKEAWAY:
  
  In open-source Iceberg (fragmented access control):
    - No native row/column-level security in Iceberg spec
    - Requires Apache Ranger, AWS Lake Formation, or custom solutions
    - Different engines may bypass policies (Spark vs Trino vs Presto)
    - Policies don't travel with the data
    - Auditing is fragmented across multiple systems
    - Row-level filtering requires view-based workarounds
  
  In Snowflake (unified governance on Iceberg):
    - Native ROW ACCESS POLICY works directly on Iceberg tables
    - Native MASKING POLICY masks columns per role/context
    - CANNOT be bypassed (enforced at platform level)
    - Same policies work on native tables AND Iceberg tables
    - Centralized audit trail (ACCESS_HISTORY)
    - Zero additional tools or infrastructure
================================================================================
*/
