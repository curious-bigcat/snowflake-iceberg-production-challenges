/*
================================================================================
  ICEBERG PRODUCTION CHALLENGES DEMO - INFRASTRUCTURE SETUP
================================================================================
  PURPOSE:  Set up all prerequisite infrastructure to demonstrate how Snowflake
            mitigates the 13 production challenges of Apache Iceberg tables.

  WHAT THIS SCRIPT DOES:
    Part A: AWS IAM Policy and Trust Relationship (reference JSON)
    Part B: Snowflake External Volume (S3 with write access)
    Part C: Database, Schema, Warehouse
    Part D: Roles and Grants (for access control challenge)
    Part E: Account/Database-level Iceberg parameter defaults
    Part F: Network Policy (IP-based access control)

  PREREQUISITES:
    - Snowflake Enterprise Edition or higher
    - ACCOUNTADMIN role
    - An S3 bucket in the same AWS region as your Snowflake account
    - AWS Console access to create IAM roles and policies

  ESTIMATED TIME: 10-15 minutes (mostly waiting for IAM trust propagation)
================================================================================
*/

-- =============================================================================
-- PART A: AWS IAM CONFIGURATION (Execute in AWS Console)
-- =============================================================================

/*
STEP 1: Create an IAM Policy named "snowflake-iceberg-demo-policy"
------------------------------------------------------------------------
Go to: AWS Console > IAM > Policies > Create Policy > JSON tab
Paste the following (replace <YOUR-BUCKET-NAME> with your actual bucket):

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SnowflakeIcebergReadWrite",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion"
            ],
            "Resource": "arn:aws:s3:::<YOUR-BUCKET-NAME>/iceberg-demo/*"
        },
        {
            "Sid": "SnowflakeIcebergListBucket",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::<YOUR-BUCKET-NAME>",
            "Condition": {
                "StringLike": {
                    "s3:prefix": "iceberg-demo/*"
                }
            }
        }
    ]
}


STEP 2: Create an IAM Role named "snowflake-iceberg-demo-role"
------------------------------------------------------------------------
Go to: AWS Console > IAM > Roles > Create Role
- Trusted entity type: "AWS Account"
- Select "Another AWS account"
- Account ID: (use your own account ID temporarily; we will update this)
- Check "Require external ID" and enter a placeholder like "0000"
- Attach the policy created in Step 1

NOTE: After running the DESCRIBE EXTERNAL VOLUME command below, you will
come back and update the trust policy with Snowflake's actual values.


STEP 3: Update the Trust Relationship (AFTER running DESCRIBE below)
------------------------------------------------------------------------
Go to: IAM > Roles > snowflake-iceberg-demo-role > Trust relationships > Edit

Replace the trust policy with:

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "<STORAGE_AWS_IAM_USER_ARN from DESCRIBE output>"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID from DESCRIBE output>"
                }
            }
        }
    ]
}

After updating, wait 30-60 seconds for IAM propagation before verifying.
*/


-- =============================================================================
-- PART B: SNOWFLAKE EXTERNAL VOLUME
-- =============================================================================

-- Use ACCOUNTADMIN for setup
USE ROLE ACCOUNTADMIN;

-- Set your S3 bucket URL and IAM role ARN here
-- IMPORTANT: Replace these values with your actual AWS details
SET s3_bucket_url = 's3://<YOUR-BUCKET-NAME>/iceberg-demo/';
SET iam_role_arn = 'arn:aws:iam::<YOUR-AWS-ACCOUNT-ID>:role/snowflake-iceberg-demo-role';

-- Create External Volume with write access (required for Snowflake-managed Iceberg)
CREATE OR REPLACE EXTERNAL VOLUME iceberg_demo_vol
  STORAGE_LOCATIONS = (
    (
      NAME = 'aws-s3-iceberg-demo'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = $s3_bucket_url
      STORAGE_AWS_ROLE_ARN = $iam_role_arn
    )
  )
  ALLOW_WRITES = TRUE;

-- Retrieve the Snowflake IAM User ARN and External ID
-- >>> USE THESE VALUES TO UPDATE THE TRUST POLICY IN STEP 3 ABOVE <<<
DESCRIBE EXTERNAL VOLUME iceberg_demo_vol;

/*
Look for these two properties in the output:
  - STORAGE_AWS_IAM_USER_ARN    (e.g., arn:aws:iam::123456789012:user/abc1-b-...)
  - STORAGE_AWS_EXTERNAL_ID     (e.g., ABC12345_SFCRole=2_f7...)

Copy them into the trust relationship JSON in Step 3, then wait 30-60 seconds.
*/

-- After updating trust policy, verify connectivity:
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('iceberg_demo_vol');

/*
Expected output: {"status":"SUCCESS"}
If you get ACCESS_DENIED, double-check:
  1. The IAM role ARN matches exactly
  2. The trust policy has the correct Snowflake IAM user ARN
  3. The external ID matches exactly (case-sensitive)
  4. The S3 bucket path matches the policy Resource
  5. Wait 60 seconds and retry (IAM propagation delay)
*/


-- =============================================================================
-- PART C: DATABASE, SCHEMA, AND WAREHOUSE
-- =============================================================================

-- Create dedicated database for the demo
CREATE OR REPLACE DATABASE ICEBERG_CHALLENGES_DB
  COMMENT = 'Demo: 13 Production Challenges of Iceberg Tables - Mitigated by Snowflake';

-- Create schema
CREATE OR REPLACE SCHEMA ICEBERG_CHALLENGES_DB.DEMO
  COMMENT = 'All demo objects live here';

-- Set context
USE DATABASE ICEBERG_CHALLENGES_DB;
USE SCHEMA DEMO;

-- Create dedicated warehouse (MEDIUM for data generation performance)
CREATE OR REPLACE WAREHOUSE ICEBERG_DEMO_WH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for Iceberg challenges demo';

USE WAREHOUSE ICEBERG_DEMO_WH;


-- =============================================================================
-- PART D: ROLES AND GRANTS (Used in Challenge 8: Access Control)
-- =============================================================================

-- Create analyst role (limited access - will have row/column restrictions)
CREATE OR REPLACE ROLE iceberg_analyst_role
  COMMENT = 'Analyst role with restricted Iceberg table access';

-- Create engineer role (full access)
CREATE OR REPLACE ROLE iceberg_engineer_role
  COMMENT = 'Engineer role with full Iceberg table access';

-- Grant basic privileges
GRANT USAGE ON DATABASE ICEBERG_CHALLENGES_DB TO ROLE iceberg_analyst_role;
GRANT USAGE ON SCHEMA ICEBERG_CHALLENGES_DB.DEMO TO ROLE iceberg_analyst_role;
GRANT USAGE ON WAREHOUSE ICEBERG_DEMO_WH TO ROLE iceberg_analyst_role;

GRANT USAGE ON DATABASE ICEBERG_CHALLENGES_DB TO ROLE iceberg_engineer_role;
GRANT USAGE ON SCHEMA ICEBERG_CHALLENGES_DB.DEMO TO ROLE iceberg_engineer_role;
GRANT USAGE ON WAREHOUSE ICEBERG_DEMO_WH TO ROLE iceberg_engineer_role;

-- Grant SELECT on future tables (so challenge tables are accessible)
GRANT SELECT ON FUTURE TABLES IN SCHEMA ICEBERG_CHALLENGES_DB.DEMO TO ROLE iceberg_analyst_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ICEBERG_CHALLENGES_DB.DEMO TO ROLE iceberg_engineer_role;
GRANT ALL ON FUTURE TABLES IN SCHEMA ICEBERG_CHALLENGES_DB.DEMO TO ROLE iceberg_engineer_role;

-- Grant roles to current user for testing
GRANT ROLE iceberg_analyst_role TO USER CURRENT_USER();
GRANT ROLE iceberg_engineer_role TO USER CURRENT_USER();


-- =============================================================================
-- PART E: ICEBERG PARAMETER DEFAULTS
-- =============================================================================

-- Set Iceberg v3 as the default version for this database
-- v3 enables deletion vectors, row lineage, and default values
ALTER DATABASE ICEBERG_CHALLENGES_DB SET ICEBERG_VERSION_DEFAULT = 3;

-- Enable merge-on-read at database level (will override per-table in demos)
ALTER DATABASE ICEBERG_CHALLENGES_DB SET ICEBERG_MERGE_ON_READ_BEHAVIOR = 'AUTO';

-- Verify current settings
SHOW PARAMETERS LIKE 'ICEBERG%' IN DATABASE ICEBERG_CHALLENGES_DB;
SHOW PARAMETERS LIKE 'DATA_RETENTION%' IN DATABASE ICEBERG_CHALLENGES_DB;


-- =============================================================================
-- PART F: NETWORK POLICY (Restrict Access to Demo Environment)
-- =============================================================================

/*
  Network policies control which IP addresses can connect to Snowflake.
  For production Iceberg deployments, this ensures:
  - Only trusted networks can access sensitive financial data
  - External Iceberg readers (Spark/Trino) connect only from known CIDRs
  - Compliance with data residency and access boundary requirements
*/

-- Create a network policy that allows access from common ranges
-- Replace these CIDRs with your actual corporate/VPN/CI-CD ranges
CREATE OR REPLACE NETWORK POLICY iceberg_demo_network_policy
  ALLOWED_IP_LIST = (
    '0.0.0.0/0'              -- DEMO ONLY: allows all IPs. Replace with your CIDRs:
    -- '10.0.0.0/8'          -- Example: internal corporate network
    -- '172.16.0.0/12'       -- Example: VPN range
    -- '192.168.1.0/24'      -- Example: office network
    -- '203.0.113.50/32'     -- Example: CI/CD runner IP
  )
  BLOCKED_IP_LIST = (
    '0.0.0.0'               -- Placeholder (required when ALLOWED is 0.0.0.0/0)
  )
  COMMENT = 'Network policy for Iceberg production challenges demo - restrict to known IPs in production';

-- View the policy
DESCRIBE NETWORK POLICY iceberg_demo_network_policy;

/*
  APPLYING NETWORK POLICIES:
  
  -- Apply to the ENTIRE account (use with caution!):
  -- ALTER ACCOUNT SET NETWORK_POLICY = iceberg_demo_network_policy;
  
  -- Apply to a specific user (safer for demos):
  -- ALTER USER <username> SET NETWORK_POLICY = iceberg_demo_network_policy;
  
  -- Apply to a specific role (for the demo roles):
  -- Network rules can also be attached via network rule objects for finer control.
  
  WARNING: Do NOT apply 'ALTER ACCOUNT SET NETWORK_POLICY' with restrictive IPs
  unless you are certain your current IP is in the allowed list, or you will
  lock yourself out of the account.
*/

-- For this demo, we create but do NOT activate the policy at account level.
-- Instead, demonstrate it's available for the access control challenge (09).

-- Create a more restrictive policy for the analyst role demonstration
CREATE OR REPLACE NETWORK POLICY iceberg_analyst_network_policy
  ALLOWED_IP_LIST = (
    '0.0.0.0/0'              -- DEMO: Replace with analyst team subnet
  )
  BLOCKED_IP_LIST = (
    '0.0.0.0'
  )
  COMMENT = 'Restrictive policy for analyst role - limit to office network in production';

-- Show created policies
SHOW NETWORK POLICIES;


-- =============================================================================
-- VERIFICATION CHECKLIST
-- =============================================================================

/*
Before proceeding to 01_generate_synthetic_data.sql, confirm:

[  ] External volume verified: SYSTEM$VERIFY_EXTERNAL_VOLUME returns SUCCESS
[  ] Database created: ICEBERG_CHALLENGES_DB
[  ] Schema created: ICEBERG_CHALLENGES_DB.DEMO
[  ] Warehouse created and usable: ICEBERG_DEMO_WH
[  ] Roles created: iceberg_analyst_role, iceberg_engineer_role
[  ] Iceberg version default set to 3
[  ] Network policies created: iceberg_demo_network_policy, iceberg_analyst_network_policy
[  ] Current context is: ICEBERG_CHALLENGES_DB.DEMO with ICEBERG_DEMO_WH

Run this to confirm context:
*/

SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE();
SHOW EXTERNAL VOLUMES LIKE 'iceberg_demo_vol';
