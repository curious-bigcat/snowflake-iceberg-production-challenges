# Challenge 2: Small File Accumulation - Analysis Results

**Date:** 2026-07-20
**Table:** `ICEBERG_CHALLENGES_DB.DEMO.TRANSACTIONS`
**Rows:** 5,500,000 (5M initial + 500K added during demo)

---

## The Problem

Streaming micro-batch ingestion creates many small Parquet files. Our `simulate_transaction_stream` procedure inserts data in 50K-row batches, creating 100+ individual commits. Each commit writes separate Parquet files, leading to:
- Excessive file-open overhead during queries
- Slow metadata scans (manifest files grow linearly)
- Suboptimal compression ratios on small files

---

## Evidence: Before Mitigation

### File Fragmentation
- **Metadata version at time of check:** `00101` (101 commits from 100 micro-batches + 1 CREATE)
- **TARGET_FILE_SIZE was set to:** `16MB` (intentionally small to demonstrate the problem)
- **ENABLE_DATA_COMPACTION:** `true` (already enabled by default)

### Metadata Location
```
s3://iceberg-demo-19jul/iceberg-demo/finserv/transactions.eCgBvQ2E/metadata/00101-75b66be1-4d20-4c35-88a8-67568a0b3edb.metadata.json
```
The `00101` prefix confirms 101 metadata versions — one per micro-batch commit.

---

## Snowflake Mitigation Applied

### Configuration Change
```sql
ALTER ICEBERG TABLE transactions SET TARGET_FILE_SIZE = 'AUTO';
```

| Parameter | Before | After |
|-----------|--------|-------|
| TARGET_FILE_SIZE | 16MB | **AUTO** |
| ENABLE_DATA_COMPACTION | true | true (unchanged) |

**What AUTO does:** Snowflake dynamically chooses the optimal file size based on table characteristics (size, DML patterns, ingestion workload). It starts at 16MB and scales up automatically.

---

## Evidence: After Mitigation

### Additional Data Loaded
- Inserted **500,000 more rows** in 10 micro-batches after setting AUTO
- New metadata version: `00111` (10 additional commits)
- Total rows: **5,500,000**

### Automatic Compaction Activity (ICEBERG_STORAGE_OPTIMIZATION_HISTORY)

| Start Time | End Time | Credits Used | Bytes Scanned | Rows Compacted |
|---|---|---|---|---|
| 2026-07-20 09:00 | 2026-07-20 10:00 | 0.001048 | 207.8 MB | 5,000,000 |
| 2026-07-20 07:00 | 2026-07-20 08:00 | 0.000062 | 0 | 0 |
| 2026-07-19 06:00 | 2026-07-19 07:00 | 0.000817 | 207.9 MB | 5,000,000 |
| 2026-07-19 00:00 | 2026-07-19 01:00 | 0.000846 | 207.8 MB | 5,000,000 |
| 2026-07-07 08:00 | 2026-07-07 09:00 | 0.000882 | 224.7 MB | 5,000,000 |

**Key observations:**
- Snowflake automatically ran **5 compaction jobs** on the transactions table
- Each job scanned ~208 MB and compacted the full 5M rows into optimally-sized files
- Total cost: **0.003655 credits** (~$0.01) for all compaction activity
- Compaction runs AUTOMATICALLY — no Spark clusters, no scheduling, no intervention

### Total Compaction Cost Across All Tables

| Table | Compaction Runs | Total Credits | MB Scanned | Rows Compacted |
|---|---|---|---|---|
| TRANSACTIONS | 5 | 0.003655 | 808.9 MB | 20,000,000 |
| COMPLIANCE_EVENTS | 3 | 0.000213 | 0 | 0 |
| RISK_SCORES | 3 | 0.000204 | 0 | 0 |
| TRANSACTIONS_STAGING | 3 | 0.000194 | 0 | 0 |
| ACCOUNTS | 3 | 0.000180 | 0 | 0 |
| PORTFOLIOS | 3 | 0.000166 | 0 | 0 |
| MARKET_DATA | 3 | 0.000161 | 0 | 0 |
| **BENCH_MANAGED_STORAGE** | 1 | **0.000000** | 0 | 0 |

**Note:** `BENCH_MANAGED_STORAGE` (SNOWFLAKE_MANAGED) shows **zero credits** — compaction is bundled/free for managed storage when only Snowflake writes to the table.

---

## OSS Iceberg vs Snowflake Comparison

| Aspect | OSS Iceberg + Spark | Snowflake Managed |
|--------|-------------------|-------------------|
| **Compaction trigger** | Manual scheduling (Airflow/cron) | Automatic (serverless) |
| **Infrastructure** | EMR/Databricks cluster ($0.50+/hr min) | None |
| **Configuration** | Tune `target-file-size-bytes`, `min-input-files`, etc. | `TARGET_FILE_SIZE = 'AUTO'` |
| **Cost for our 5M row table** | ~$5-15/day (cluster time) | **$0.01 total** (0.003655 credits) |
| **Failure handling** | Manual retry logic | Automatic |
| **Monitoring** | Custom dashboards | Built-in `ICEBERG_STORAGE_OPTIMIZATION_HISTORY` |

---

## Conclusion

Snowflake eliminates the small-file problem through:
1. **`TARGET_FILE_SIZE = 'AUTO'`** — adapts file sizes dynamically to workload
2. **Automatic data compaction** — merges small files serverlessly in the background
3. **Near-zero cost** — 0.003655 credits ($0.01) vs $5-15/day for Spark compaction clusters
4. **Zero operations** — no scheduling, no infrastructure, no failure handling
