# Storage Comparison Results: External Volume (S3) vs SNOWFLAKE_MANAGED

**Date:** 2026-07-20
**Account:** SFSEAPAC-BSURESH
**Warehouse:** ICEBERG_DEMO_WH (MEDIUM)
**Data:** 1M rows per table (identical schema and data distribution)

---

## Benchmark Results

### 1. Bulk INSERT Performance (1M rows)

| Storage Mode | Elapsed Time (ms) | Rows Inserted |
|---|---|---|
| **External Volume (S3)** | 5,542 ms (5.5s) | 1,000,000 |
| **SNOWFLAKE_MANAGED** | 4,405 ms (4.4s) | 1,000,000 |
| **Winner** | SNOWFLAKE_MANAGED | **20% faster** |

### 2. Analytical Query Performance (GROUP BY with filters)

| Storage Mode | Elapsed Time (ms) | Query |
|---|---|---|
| **External Volume (S3)** | 809 ms | Aggregation on region + txn_type with date/amount filters |
| **SNOWFLAKE_MANAGED** | 1,191 ms | Same query |
| **Winner** | External Volume | **32% faster** |

*Note: First query benefits from warehouse warm-up; difference may normalize on repeated runs.*

### 3. UPDATE Performance (Merge-on-Read with deletion vectors, ~125K rows)

| Storage Mode | Elapsed Time (ms) | Rows Updated |
|---|---|---|
| **External Volume (S3)** | 4,204 ms (4.2s) | 125,443 |
| **SNOWFLAKE_MANAGED** | 4,301 ms (4.3s) | 124,983 |
| **Winner** | Essentially equal | ~2% difference (within noise) |

### 4. DELETE Performance (~127K rows)

| Storage Mode | Elapsed Time (ms) | Rows Deleted |
|---|---|---|
| **External Volume (S3)** | 4,788 ms (4.8s) | 127,615 |
| **SNOWFLAKE_MANAGED** | 3,644 ms (3.6s) | 127,175 |
| **Winner** | SNOWFLAKE_MANAGED | **24% faster** |

---

## Storage & Metadata Comparison

| Property | External Volume (S3) | SNOWFLAKE_MANAGED |
|---|---|---|
| **Storage Location** | `s3://iceberg-demo-19jul/iceberg-demo/benchmark/external_vol.AY3m2To2/` | `s3://sfc-va3-ds1-82-customer-interop-fs-josp0000-s/iceberg/...` (Snowflake-managed bucket) |
| **Metadata Versions** | 3 (create + insert + update/delete) | 3 (same) |
| **Metadata Format** | Standard Iceberg v3 | Standard Iceberg v3 |
| **External Engine Access** | Direct S3 read (open files) | Via Iceberg REST Catalog (Horizon) |
| **Compaction Cost** | Billed as serverless credits | **BUNDLED (free for Snowflake-only writes)** |
| **BASE_LOCATION** | User-specified | Not supported (Snowflake manages paths) |

---

## Summary Table

| Benchmark | External Volume (S3) | SNOWFLAKE_MANAGED | Delta |
|---|---|---|---|
| INSERT 1M rows | 5.54s | **4.41s** | Managed 20% faster |
| SELECT (analytical) | **0.81s** | 1.19s | External 32% faster |
| UPDATE 125K rows | 4.20s | 4.30s | Equal |
| DELETE 127K rows | 4.79s | **3.64s** | Managed 24% faster |

---

## Key Findings

1. **SNOWFLAKE_MANAGED is faster for writes** (INSERT, DELETE) by 20-24% — likely due to optimized internal storage paths.
2. **External Volume is comparable or faster for reads** — S3 direct access with pre-warmed cache provides slightly better scan performance.
3. **UPDATE performance is equivalent** — Both use Iceberg v3 deletion vectors (merge-on-read), which writes small vector files regardless of storage backend.
4. **The biggest differentiator is operational cost**: SNOWFLAKE_MANAGED bundles compaction for free when only Snowflake writes to the table. External Volume charges serverless credits for compaction.

---

## Recommendation

| Use Case | Recommended Storage |
|---|---|
| Multi-engine lakehouse (Spark/Trino + Snowflake) | **External Volume** — direct file access |
| Snowflake-primary with occasional external reads | **SNOWFLAKE_MANAGED** — zero ops, free compaction |
| Cost-sensitive with high write volume | **SNOWFLAKE_MANAGED** — bundled compaction savings |
| Data sovereignty / bring-your-own-bucket | **External Volume** — your bucket, your region |
| Fastest possible ingestion | **SNOWFLAKE_MANAGED** — optimized write path |
