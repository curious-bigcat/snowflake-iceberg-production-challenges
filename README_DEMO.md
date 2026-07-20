# Iceberg Production Challenges Demo

**Technical deep-dive demonstrating how Snowflake mitigates the 13 production challenges of Apache Iceberg tables.**

## Domain: Financial Services (Investment Banking / Wealth Management)

- 7 interconnected tables (~10M+ total rows)
- Production-scale data (5M transactions, 2M market ticks, 500K accounts)
- Realistic streaming, DML, and CDC patterns

---

## Prerequisites

| Requirement | Details |
|---|---|
| Snowflake Edition | Enterprise or higher |
| Role | ACCOUNTADMIN |
| AWS | S3 bucket + IAM role (see `00_setup_infrastructure.sql`) |
| Warehouse | MEDIUM or larger recommended |
| Time | ~45-60 minutes for full demo |

---

## File Execution Order

| # | File | Description | Time |
|---|------|-------------|------|
| 0 | `00_setup_infrastructure.sql` | External volume, IAM, DB, schema, warehouse, roles | 10 min |
| 1 | `01_generate_synthetic_data.sql` | 7-table Financial Services model (~10M rows) | 5-10 min |
| 1b | `01b_storage_comparison.sql` | External Volume vs SNOWFLAKE_MANAGED benchmark | 5 min |
| 2 | `02_challenge_small_files.sql` | Small file accumulation -> Auto compaction | 3 min |
| 3 | `03_challenge_metadata_bloat.sql` | Metadata bloating -> Auto manifest compaction | 3 min |
| 4 | `04_challenge_compaction_costs.sql` | Compaction compute -> Serverless optimization | 3 min |
| 5 | `05_challenge_copy_on_write.sql` | COW latency -> Deletion vectors (v3 MOR) | 5 min |
| 6 | `06_challenge_merge_on_read.sql` | MOR read penalty -> Auto compaction healing | 4 min |
| 7 | `07_challenge_concurrency.sql` | Commit conflicts -> Native MVCC | 4 min |
| 8 | `08_challenge_catalog_sync.sql` | Catalog drift -> AUTO_REFRESH / managed catalog | 3 min |
| 9 | `09_challenge_access_control.sql` | Fragmented security -> Native policies | 5 min |
| 10 | `10_challenge_orphan_files.sql` | Orphan files -> Auto snapshot expiry | 3 min |
| 11 | `11_challenge_manual_cleanup.sql` | Manual maintenance -> Zero-ops automation | 3 min |
| 12 | `12_challenge_sql_support.sql` | Inconsistent DML -> Full SQL parity | 4 min |
| 13 | `13_challenge_missing_indexes.sql` | No indexes -> Automatic Clustering | 4 min |
| 14 | `14_challenge_version_mismatch.sql` | Version chaos -> Centralized version control | 4 min |
| 99 | `99_teardown.sql` | Remove all demo objects | 1 min |

---

## Challenge-to-Mitigation Mapping

| # | Production Challenge | Snowflake Feature | Key Config |
|---|---------------------|-------------------|-----------|
| 1 | Small file accumulation | Auto data compaction + TARGET_FILE_SIZE | `TARGET_FILE_SIZE = 'AUTO'` |
| 2 | Metadata file bloating | Auto manifest compaction (always-on, free) | Built-in |
| 3 | Compaction compute costs | Serverless optimization (no Spark) | Monitor via `ICEBERG_STORAGE_OPTIMIZATION_HISTORY` |
| 4 | Copy-on-Write latency | Deletion vectors (Iceberg v3) | `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ENABLED'` |
| 5 | Merge-on-Read penalties | Auto compaction heals read penalty | `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'AUTO'` |
| 6 | Commit concurrency conflicts | Native MVCC (no Iceberg-level retries) | Multi-statement transactions |
| 7 | Catalog synchronization drift | AUTO_REFRESH / Snowflake-managed catalog | `AUTO_REFRESH = TRUE` |
| 8 | Fragmented access control | Native Row Access + Masking Policies | `ROW ACCESS POLICY`, `MASKING POLICY` |
| 9 | Orphan file accumulation | Auto snapshot expiry | `DATA_RETENTION_TIME_IN_DAYS` |
| 10 | Manual storage cleanup | Fully automated (zero scheduling) | All automatic |
| 11 | Inconsistent SQL support | Full DML parity | INSERT, UPDATE, DELETE, MERGE, TRUNCATE |
| 12 | Missing platform indexes | Automatic Clustering | `CLUSTER BY (col1, col2)` |
| 13 | Format version mismatches | Centralized version control | `ICEBERG_VERSION_DEFAULT`, per-table override |

---

## Data Model (Financial Services)

```
                    ┌─────────────────────────────────────────┐
                    │              ACCOUNTS                    │
                    │            (500K rows)                   │
                    │  Dimension: customers, PII, tiers       │
                    └────────────┬────────────────────────────┘
                                 │
          ┌──────────────────────┼──────────────────────────┐
          │                      │                          │
          ▼                      ▼                          ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐
│  TRANSACTIONS    │  │   RISK_SCORES    │  │  COMPLIANCE_EVENTS   │
│   (5M rows)      │  │    (2M rows)     │  │     (1M rows)        │
│  Streaming ingest│  │  Daily updates   │  │   Append-only audit  │
└──────────────────┘  └──────────────────┘  └──────────────────────┘
          │
          ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐
│  TXN_STAGING     │  │   PORTFOLIOS     │  │    MARKET_DATA       │
│  (100K rows)     │  │   (200K rows)    │  │     (2M rows)        │
│  CDC for MERGE   │  │  DML-heavy holds │  │  High-freq ticks     │
└──────────────────┘  └──────────────────┘  └──────────────────────┘
```

---

## Storage Mode Comparison (01b)

The `01b_storage_comparison.sql` file benchmarks two storage architectures:

| | External Volume (S3) | SNOWFLAKE_MANAGED |
|---|---|---|
| Storage | Your S3 bucket | Snowflake internal |
| Billing | AWS S3 costs | Snowflake credits |
| Compaction cost | Serverless credits | BUNDLED (free) |
| External engine access | Direct S3 read | Via Iceberg REST Catalog |
| Operational overhead | Manage bucket, IAM | Zero |
| Best for | Multi-engine lakehouse | Snowflake-primary workloads |

---

## Quick Start

```sql
-- 1. Open Snowsight
-- 2. Run files in order: 00 -> 01 -> 01b -> 02 through 14
-- 3. Each file is self-contained and can be run independently
-- 4. Run 99_teardown.sql when done
```

---

## Post-Demo Cleanup

After running `99_teardown.sql`, manually:
1. Delete S3 prefix: `aws s3 rm s3://<bucket>/iceberg-demo/ --recursive`
2. Delete IAM role: `snowflake-iceberg-demo-role`
3. Delete IAM policy: `snowflake-iceberg-demo-policy`
