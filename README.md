# Snowflake Iceberg Production Challenges

**Runnable demo proving how Snowflake mitigates all 13 production challenges of Apache Iceberg tables.**

A technical deep-dive with a 10M-row Financial Services data model, side-by-side storage benchmarks, and live execution results showing 99.7% compaction cost reduction, 60% faster writes with deletion vectors, and zero-ops maintenance.

---

## Why This Exists

Apache Iceberg is the leading open table format for data lakehouses. But running Iceberg in **production** creates significant operational pain:

| # | Production Challenge | What Goes Wrong |
|---|---------------------|-----------------|
| 1 | Small file accumulation | Streaming creates millions of tiny files |
| 2 | Metadata file bloating | Every commit adds manifest files |
| 3 | Compaction compute costs | Requires dedicated Spark clusters |
| 4 | Copy-on-Write latency | Rewrites entire files for 1-row updates |
| 5 | Merge-on-Read penalties | Delete files degrade read performance |
| 6 | Commit concurrency conflicts | Parallel writers trigger retry loops |
| 7 | Catalog synchronization drift | Multi-platform metadata goes stale |
| 8 | Fragmented access control | No native row/column security |
| 9 | Orphan file accumulation | Failed writes leave abandoned files |
| 10 | Manual storage cleanup | Engineers schedule 4+ maintenance jobs |
| 11 | Inconsistent SQL support | Different engines support different DML |
| 12 | Missing platform indexes | No clustering or search optimization |
| 13 | Format version mismatches | v2/v3 incompatibilities break queries |

This repo demonstrates that **Snowflake eliminates all 13 challenges** through native platform capabilities.

---

## Key Results (Live Execution)

| Metric | Result |
|--------|--------|
| Compaction cost (20M rows) | **$0.015** vs $5-15 with Spark (99.7% savings) |
| Write speed (MOR vs COW) | **60% faster** with Iceberg v3 deletion vectors |
| Read degradation after updates | **0%** (zero penalty from deletion vectors) |
| Commit conflicts | **0** across all concurrent writers |
| MERGE throughput | **90,000 rows** (70K insert + 20K update) in single statement |
| Maintenance operations needed | **0** (all 4 OSS tasks fully automated) |
| Security policies on Iceberg | **3 active** (row filter + 2 column masking) |

---

## Data Model

**Domain:** Financial Services (Investment Banking / Wealth Management)

```
ACCOUNTS (500K) ──> TRANSACTIONS (5M, streaming micro-batches)
                ──> RISK_SCORES (2M, daily recalculations)
                ──> COMPLIANCE_EVENTS (1M, audit trail)
                ──> PORTFOLIOS (200K, frequent rebalancing)

MARKET_DATA (2M, high-frequency ticks) ── joined via symbol ──> PORTFOLIOS
TRANSACTIONS_STAGING (100K) ── CDC feed for MERGE demos
```

Total: **~10.8M rows** across 7 interconnected Iceberg tables.

---

## Repository Structure

```
├── 00_setup_infrastructure.sql          # IAM, external volume, DB, roles, network policy
├── 01_generate_synthetic_data.sql       # 7-table Financial Services model (~10M rows)
├── 01b_storage_comparison.sql           # External Volume vs SNOWFLAKE_MANAGED benchmark
├── 02_challenge_small_files.sql         # Challenge 1: Auto compaction
├── 03_challenge_metadata_bloat.sql      # Challenge 2: Manifest compaction
├── 04_challenge_compaction_costs.sql    # Challenge 3: Serverless optimization
├── 05_challenge_copy_on_write.sql       # Challenge 4: Deletion vectors (v3)
├── 06_challenge_merge_on_read.sql       # Challenge 5: Self-healing reads
├── 07_challenge_concurrency.sql         # Challenge 6: Native MVCC
├── 08_challenge_catalog_sync.sql        # Challenge 7: AUTO_REFRESH
├── 09_challenge_access_control.sql      # Challenge 8: Row/column policies
├── 10_challenge_orphan_files.sql        # Challenge 9: Auto snapshot expiry
├── 11_challenge_manual_cleanup.sql      # Challenge 10: Zero-ops automation
├── 12_challenge_sql_support.sql         # Challenge 11: Full DML parity
├── 13_challenge_missing_indexes.sql     # Challenge 12: Automatic Clustering
├── 14_challenge_version_mismatch.sql    # Challenge 13: Version governance
├── 99_teardown.sql                      # Cleanup all demo objects
├── README.md                            # This file
├── README_DEMO.md                       # Detailed execution guide
└── results/                             # Live execution results
    ├── 00_EXECUTIVE_SUMMARY.md
    ├── 01b_storage_comparison_results.md
    ├── 02_small_files_analysis.md
    ├── 03_metadata_bloat_analysis.md
    ├── 04_compaction_costs_analysis.md
    ├── 05_copy_on_write_analysis.md
    ├── 06_merge_on_read_analysis.md
    ├── 07_concurrency_analysis.md
    ├── 08_catalog_sync_analysis.md
    ├── 09_access_control_analysis.md
    └── 10_to_14_combined_analysis.md
```

---

## Prerequisites

| Requirement | Details |
|---|---|
| Snowflake Edition | Enterprise or higher |
| Role | ACCOUNTADMIN |
| AWS | S3 bucket + IAM role (full setup in `00_setup_infrastructure.sql`) |
| Warehouse | MEDIUM or larger |
| Time | ~45-60 minutes for full demo |

---

## Quick Start

```sql
-- 1. Open Snowsight
-- 2. Update S3 bucket name and IAM role ARN in 00_setup_infrastructure.sql
-- 3. Run files in order: 00 -> 01 -> 01b -> 02 through 14
-- 4. Each challenge file is self-contained with PROBLEM / MITIGATION / DEMO / VERIFY sections
-- 5. Run 99_teardown.sql when done
```

---

## Storage Mode Comparison

The `01b_storage_comparison.sql` benchmarks two architectures:

| Operation (1M rows) | External Volume (S3) | SNOWFLAKE_MANAGED |
|---------------------|---------------------|-------------------|
| Bulk INSERT | 5.54s | **4.41s** (20% faster) |
| Analytical SELECT | **0.81s** | 1.19s |
| UPDATE 125K rows | 4.20s | 4.30s (tied) |
| DELETE 127K rows | 4.79s | **3.64s** (24% faster) |
| Compaction cost | Billed | **FREE (bundled)** |

**Use External Volume** for multi-engine lakehouses. **Use SNOWFLAKE_MANAGED** for Snowflake-primary workloads with zero ops.

---

## Challenge-to-Feature Mapping

| # | Challenge | Snowflake Feature | Key Configuration |
|---|-----------|-------------------|-------------------|
| 1 | Small files | Auto data compaction | `TARGET_FILE_SIZE = 'AUTO'` |
| 2 | Metadata bloat | Manifest compaction | Built-in (always-on, free) |
| 3 | Compaction costs | Serverless optimization | `ICEBERG_STORAGE_OPTIMIZATION_HISTORY` |
| 4 | COW latency | Deletion vectors (v3) | `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'ENABLED'` |
| 5 | MOR read penalty | Auto compaction healing | `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'AUTO'` |
| 6 | Concurrency | Native MVCC | `BEGIN TRANSACTION` / `COMMIT` |
| 7 | Catalog drift | Managed catalog | `AUTO_REFRESH = TRUE` |
| 8 | Access control | Native policies | `ROW ACCESS POLICY`, `MASKING POLICY` |
| 9 | Orphan files | Snapshot expiry | `DATA_RETENTION_TIME_IN_DAYS` |
| 10 | Manual cleanup | Full automation | Zero configuration |
| 11 | SQL support | Full DML | INSERT, UPDATE, DELETE, MERGE, TRUNCATE |
| 12 | Missing indexes | Automatic Clustering | `CLUSTER BY (col1, col2)` |
| 13 | Version mismatch | Version governance | `ICEBERG_VERSION_DEFAULT = 3` |

---

## Cleanup

```sql
-- Run 99_teardown.sql to remove all Snowflake objects, then:
aws s3 rm s3://<bucket>/iceberg-demo/ --recursive
-- Delete IAM role: snowflake-iceberg-demo-role
-- Delete IAM policy: snowflake-iceberg-demo-policy
```

---

## License

MIT
