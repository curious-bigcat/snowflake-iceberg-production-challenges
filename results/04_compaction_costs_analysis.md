# Challenge 4: Compaction Compute Costs - Analysis Results

**Date:** 2026-07-20
**Scope:** All tables in ICEBERG_CHALLENGES_DB

---

## The Problem

In open-source Iceberg, data compaction (merging small files into larger ones) requires:
- Dedicated Spark/Flink/EMR clusters ($0.50+/hour minimum)
- Scheduling infrastructure (Airflow, Step Functions, cron)
- Manual tuning of bin-pack strategy, parallelism, memory
- Paying for idle compute when no compaction is needed
- Typical cost: 20-40% of total Iceberg compute budget

---

## Snowflake's Solution: Serverless Compaction

Compaction runs as a **serverless service** — no clusters to provision, pay only for actual work.

---

## Actual Costs Measured

### Total Compaction Cost (All Tables, All Time)

| Metric | Value |
|--------|-------|
| **Total credits consumed** | 0.004875 |
| **Estimated USD cost** | **$0.015** (at $3/credit) |
| **Total GB processed** | 0.79 GB |
| **Total rows compacted** | 20,000,000 |
| **Total jobs executed** | 26 |

**For 20 million rows compacted, total cost is $0.015 (1.5 cents).**

### Daily Cost Breakdown

| Date | Jobs | Credits | GB Scanned | Rows Compacted |
|------|------|---------|------------|----------------|
| 2026-07-20 | 2 | 0.001110 | 0.19 GB | 5,000,000 |
| 2026-07-19 | 17 | 0.002499 | 0.39 GB | 10,000,000 |
| 2026-07-07 | 7 | 0.001266 | 0.21 GB | 5,000,000 |

### Per-Table Breakdown

| Table | Runs | Credits | Rows Compacted | Cost/Run |
|-------|------|---------|----------------|----------|
| TRANSACTIONS | 5 | 0.003655 | 20,000,000 | $0.002/run |
| COMPLIANCE_EVENTS | 3 | 0.000213 | 0 | maintenance only |
| RISK_SCORES | 3 | 0.000204 | 0 | maintenance only |
| Others | 3 each | ~0.0002 | 0 | maintenance only |
| **BENCH_MANAGED_STORAGE** | 1 | **0.000000** | 0 | **FREE (bundled)** |

---

## Cost Comparison: OSS Spark vs Snowflake

| Aspect | OSS Iceberg + Spark | Snowflake Serverless |
|--------|-------------------|---------------------|
| **Minimum cost** | ~$0.50/hr (smallest EMR cluster) | $0 when idle |
| **Cost for our 20M row compaction** | ~$5-15 (cluster time + scheduling) | **$0.015** |
| **Savings** | — | **99.7% cheaper** |
| **Idle cost** | Cluster running 24/7 or cold-start delay | Zero |
| **Scheduling infrastructure** | Airflow/cron ($$$) | None needed |
| **Failed job cost** | Wasted cluster time + retry | Automatic retry, no waste |
| **Monitoring** | Custom Grafana/Datadog | Built-in ACCOUNT_USAGE view |

### Projected Annual Comparison (for a 10-table production lakehouse)

| Scenario | OSS Spark Compaction | Snowflake |
|----------|---------------------|-----------|
| Small (10 tables, 100M rows) | $150-500/month | ~$5/month |
| Medium (50 tables, 1B rows) | $500-2000/month | ~$25/month |
| Large (200 tables, 10B rows) | $2000-8000/month | ~$100/month |

---

## Key Control: Disable/Enable Per Table

```sql
-- Disable for rarely-queried archive tables (save cost)
ALTER ICEBERG TABLE archive_table SET ENABLE_DATA_COMPACTION = FALSE;

-- Re-enable when needed
ALTER ICEBERG TABLE archive_table SET ENABLE_DATA_COMPACTION = TRUE;

-- SNOWFLAKE_MANAGED tables: compaction is BUNDLED (free)
-- when only Snowflake writes to the table
```

---

## Conclusion

Snowflake's serverless compaction costs **$0.015 for 20M rows** compared to **$5-15 for equivalent Spark-based compaction**. The service:
1. Runs automatically (no scheduling)
2. Scales to zero when idle (no minimum cost)
3. Is FREE for SNOWFLAKE_MANAGED storage tables
4. Can be disabled per-table for cost optimization
5. Provides full transparency via `ICEBERG_STORAGE_OPTIMIZATION_HISTORY`
