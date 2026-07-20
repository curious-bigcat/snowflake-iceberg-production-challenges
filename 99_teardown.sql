/*
================================================================================
  ICEBERG PRODUCTION CHALLENGES DEMO - TEARDOWN
================================================================================
  PURPOSE:  Remove all demo objects created by this demo suite.
  
  WARNING:  This script PERMANENTLY deletes all data and objects.
            Only run when you are completely done with the demo.

  OBJECTS REMOVED:
    - All Iceberg tables in ICEBERG_CHALLENGES_DB.DEMO
    - Database: ICEBERG_CHALLENGES_DB
    - External Volume: iceberg_demo_vol
    - Warehouse: ICEBERG_DEMO_WH
    - Roles: iceberg_analyst_role, iceberg_engineer_role
    - Policies: region_filter_policy, ssn_mask, salary_mask

  NOTE: This does NOT clean up S3 bucket contents. After running this,
        manually delete the /iceberg-demo/ prefix from your S3 bucket,
        or wait for Snowflake's async cleanup to remove metadata files.
================================================================================
*/

USE ROLE ACCOUNTADMIN;


-- =============================================================================
-- STEP 1: Drop policies (must be done before dropping tables)
-- =============================================================================

-- Remove policies from tables first (required before drop)
ALTER ICEBERG TABLE IF EXISTS ICEBERG_CHALLENGES_DB.DEMO.EMPLOYEE_COMPENSATION
  DROP ROW ACCESS POLICY IF EXISTS ICEBERG_CHALLENGES_DB.DEMO.REGION_FILTER_POLICY;

ALTER ICEBERG TABLE IF EXISTS ICEBERG_CHALLENGES_DB.DEMO.EMPLOYEE_COMPENSATION
  MODIFY COLUMN SSN UNSET MASKING POLICY;

ALTER ICEBERG TABLE IF EXISTS ICEBERG_CHALLENGES_DB.DEMO.EMPLOYEE_COMPENSATION
  MODIFY COLUMN SALARY UNSET MASKING POLICY;

-- Drop policies
DROP ROW ACCESS POLICY IF EXISTS ICEBERG_CHALLENGES_DB.DEMO.REGION_FILTER_POLICY;
DROP MASKING POLICY IF EXISTS ICEBERG_CHALLENGES_DB.DEMO.SSN_MASK;
DROP MASKING POLICY IF EXISTS ICEBERG_CHALLENGES_DB.DEMO.SALARY_MASK;


-- =============================================================================
-- STEP 2: Drop database (cascades to all schemas, tables, procedures)
-- =============================================================================

DROP DATABASE IF EXISTS ICEBERG_CHALLENGES_DB;


-- =============================================================================
-- STEP 3: Drop external volume
-- =============================================================================

DROP EXTERNAL VOLUME IF EXISTS iceberg_demo_vol;


-- =============================================================================
-- STEP 4: Drop warehouse
-- =============================================================================

DROP WAREHOUSE IF EXISTS ICEBERG_DEMO_WH;


-- =============================================================================
-- STEP 5: Drop roles
-- =============================================================================

DROP ROLE IF EXISTS iceberg_analyst_role;
DROP ROLE IF EXISTS iceberg_engineer_role;


-- =============================================================================
-- STEP 6: Drop network policies
-- =============================================================================

DROP NETWORK POLICY IF EXISTS iceberg_demo_network_policy;
DROP NETWORK POLICY IF EXISTS iceberg_analyst_network_policy;


-- =============================================================================
-- STEP 7: Reset any account-level parameters (optional)
-- =============================================================================

-- Uncomment if you changed account-level defaults during the demo:
-- ALTER ACCOUNT UNSET ICEBERG_VERSION_DEFAULT;
-- ALTER ACCOUNT UNSET ICEBERG_MERGE_ON_READ_BEHAVIOR;


-- =============================================================================
-- VERIFICATION
-- =============================================================================

-- Confirm everything is gone
SHOW DATABASES LIKE 'ICEBERG_CHALLENGES_DB';
SHOW EXTERNAL VOLUMES LIKE 'iceberg_demo_vol';
SHOW WAREHOUSES LIKE 'ICEBERG_DEMO_WH';
SHOW ROLES LIKE 'iceberg_%_role';


/*
================================================================================
  CLEANUP COMPLETE
================================================================================
  
  REMAINING MANUAL STEPS:
  
  1. AWS S3: Delete the /iceberg-demo/ prefix from your S3 bucket
     (Snowflake deletes metadata asynchronously but some files may remain)
     
     aws s3 rm s3://<YOUR-BUCKET>/iceberg-demo/ --recursive
  
  2. AWS IAM: Delete the IAM role and policy created for this demo
     - Role: snowflake-iceberg-demo-role
     - Policy: snowflake-iceberg-demo-policy
  
  3. Verify no orphan costs in your AWS bill after 24-48 hours

================================================================================
*/
