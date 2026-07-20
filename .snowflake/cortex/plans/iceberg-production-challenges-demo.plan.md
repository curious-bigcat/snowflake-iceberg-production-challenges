# Plan: Iceberg Production Challenges Demo on Snowflake

## Overview

Create a single, comprehensive SQL script (`iceberg_production_challenges_demo.sql`) that walks through all 13 production challenges of Apache Iceberg tables and demonstrates how Snowflake's managed Iceberg platform mitigates each one. The script uses synthetic IoT streaming data and is designed to run sequentially in Snowsight.

---

## Architecture

```
iceberg_production_challenges_demo.sql
├── SECTION 0: Setup (External Volume, DB, Schema, Warehouse)
├── SECTION 1: Synthetic Data Generator (Stored Procedure)
├── SECTION 2-14: One section per challenge (13 challenges)
│   └── Each section:
│       ├── -- CHALLENGE: Description of the problem
│       ├── -- SNOWFLAKE MITIGATION: How Snowflake solves it
│       ├── -- DEMO: Runnable SQL showing the feature
│       └── -- VERIFY: Query proving the mitigation works
└── SECTION 15: Cleanup (optional teardown)
```

---

## Challenge-to-Feature Mapping

| # | Production Challenge | Snowflake Mitigation | Key Configuration |
|---|---------------------|---------------------|-------------------|
| 1 | Small file accumulation | Automatic data compaction + TARGET_FILE_SIZE | `TARGET_FILE_SIZE = 'AUTO'`, `ENABLE_DATA_COMPACTION = TRUE` |
| 2 | Metadata file bloating | Automatic manifest compaction (always-on, zero cost) | Built-in, cannot be disabled |
| 3 | Compaction compute costs | Serverless table optimization (no user-managed Spark) | Automatic; monitor via `ICEBERG_STORAGE_OPTIMIZATION_HISTORY` |
| 4 | Copy-on-Write latency | Configurable merge-on-read with deletion vectors (v3) | `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ENABLED'` |
| 5 | Merge-on-Read penalties | Automatic heuristic switching + background compaction | `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'AUTO'` with compaction |
| 6 | Commit concurrency conflicts | Snowflake's native MVCC (no Iceberg-level commit retries) | Multi-statement transactions, no user config needed |
| 7 | Catalog synchronization drift | AUTO_REFRESH with event notifications | `AUTO_REFRESH = TRUE` or Snowflake-managed catalog |
| 8 | Fragmented access control | Native row access policies + column masking on Iceberg | `ROW ACCESS POLICY`, `MASKING POLICY` applied directly |
| 9 | Orphan file accumulation | Automatic snapshot expiry cleans unreferenced files | `DATA_RETENTION_TIME_IN_DAYS`; detection via `TABLE_STORAGE_METRICS` |
| 10 | Manual storage cleanup | Fully automated snapshot expiry + data compaction | Zero manual scheduling required |
| 11 | Inconsistent SQL support | Full DML (INSERT, UPDATE, DELETE, MERGE, TRUNCATE) | Works identically to native Snowflake tables |
| 12 | Missing platform indexes | Automatic Clustering + Search Optimization on Iceberg | `CLUSTER BY (col)`, `ALTER TABLE ... ADD SEARCH OPTIMIZATION` |
| 13 | Format version mismatches | Account-level Iceberg version default + per-table override | `ICEBERG_VERSION_DEFAULT`, `ICEBERG_VERSION = 3` |

---

## Detailed Implementation

### Section 0: Infrastructure Setup

```sql
-- External Volume pointing to S3 (user provides bucket/IAM)
CREATE OR REPLACE EXTERNAL VOLUME iceberg_demo_vol ...;
-- Database and schema
CREATE OR REPLACE DATABASE iceberg_challenges_db;
CREATE OR REPLACE SCHEMA iceberg_challenges_db.demo;
```

**Note:** The external volume requires a real S3 bucket with IAM trust policy. The script will include a placeholder with instructions, plus a `SNOWFLAKE_MANAGED` fallback option for accounts that support it.

### Section 1: Synthetic Data Generator

A stored procedure `generate_iot_data(num_records INT, num_batches INT)` that:
- Creates an Iceberg table `sensor_readings` with columns: `device_id`, `sensor_type`, `reading_value`, `event_ts`, `region`
- Inserts data in many small batches (simulating streaming micro-batches)
- Creates enough volume to demonstrate compaction and clustering

### Sections 2-14: Challenge Demonstrations

Each section follows this pattern:
1. **Comment block** explaining the challenge
2. **Configuration SQL** showing the Snowflake parameter/feature
3. **Demo operations** (DML, DDL, queries)
4. **Verification query** proving the mitigation

### Section 15: Cleanup

Optional `DROP` statements gated by a variable.

---

## Prerequisites

- Snowflake Enterprise Edition or higher (for row access policies, clustering)
- An S3 bucket with Snowflake IAM trust policy configured (or use `SNOWFLAKE_MANAGED` external volume)
- ACCOUNTADMIN or equivalent role for parameter changes
- A warehouse (MEDIUM recommended for data generation)

---

## Key Design Decisions

1. **Single file**: Everything in one `.sql` file for easy sharing and Snowsight execution
2. **SNOWFLAKE_MANAGED fallback**: For demos without external S3, use Snowflake-managed storage
3. **Synthetic data**: IoT sensor readings — universally understood, demonstrates streaming patterns
4. **Non-destructive**: Account-level parameters are shown but commented out; only database/schema-level settings are actually applied
5. **Idempotent**: Uses `CREATE OR REPLACE` throughout so the script can be re-run

---

## Output

A single file: `/Users/bsuresh/Documents/Projects/iceberg_challenge/iceberg_production_challenges_demo.sql`

~500-700 lines of well-commented SQL covering all 13 challenges with working demonstrations.
