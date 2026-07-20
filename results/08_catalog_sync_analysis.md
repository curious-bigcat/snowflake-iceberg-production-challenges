# Challenge 8: Catalog Synchronization Drift - Analysis Results

**Date:** 2026-07-20

---

## The Problem

When Iceberg tables are managed by an external catalog (Glue, Unity Catalog, Polaris), keeping metadata synced across platforms is error-prone:
- External writes update the catalog but Snowflake has stale metadata
- Manual REFRESH commands needed after every external change
- Missed refreshes = queries returning outdated data
- Complex event notification infrastructure (SNS/SQS for AWS)

---

## Snowflake's Mitigation: Three Approaches

### Approach 1: Snowflake-Managed Catalog (Zero Drift)

When `CATALOG = 'SNOWFLAKE'`, there is **no synchronization problem**:
- Snowflake IS the catalog (single source of truth)
- All writes go through Snowflake → metadata always consistent
- Reads immediately see latest committed data

**Demonstrated:**
```sql
INSERT INTO transactions VALUES (...);  -- Write
SELECT * FROM transactions WHERE ...;    -- Immediately visible (no refresh!)
```
Result: Row inserted and queryable in the **same statement batch** — zero lag.

### Approach 2: AUTO_REFRESH for External Catalogs

For tables managed by Glue/Unity/Polaris:
```sql
CREATE ICEBERG TABLE external_events
  CATALOG = my_glue_integration
  AUTO_REFRESH = TRUE;  -- Event-driven automatic sync
```
Snowflake listens for SNS/SQS notifications and refreshes automatically.

### Approach 3: Catalog-Linked Databases (Auto-Discovery)

For syncing an entire external catalog:
```sql
CREATE DATABASE my_lakehouse
  LINKED_CATALOG = my_glue_integration
  AUTO_REFRESH = TRUE;
-- All tables auto-discovered and synced
```

---

## External Consumer Access

Snowflake generates standard Iceberg metadata for external readers:

| Table | Metadata Location |
|-------|-------------------|
| transactions | `s3://iceberg-demo-19jul/.../metadata/00113-...metadata.json` |
| compliance_events | `s3://iceberg-demo-19jul/.../metadata/00001-...metadata.json` |

External engines (Spark/Trino) can read these tables via the S3 metadata path — **Snowflake is both producer and catalog**.

---

## Comparison

| Scenario | OSS Iceberg | Snowflake |
|----------|------------|-----------|
| Snowflake as primary engine | N/A | Zero drift (CATALOG = 'SNOWFLAKE') |
| External writes | Manual REFRESH per table | AUTO_REFRESH = TRUE |
| Multi-table sync | Script per table | Catalog-Linked Database |
| External consumers reading Snowflake data | Complex catalog setup | SYSTEM$GET_ICEBERG_TABLE_INFORMATION |
| New tables added externally | Manual registration | Auto-discovered via CLD |

---

## Conclusion

Snowflake eliminates catalog synchronization drift through:
1. **CATALOG = 'SNOWFLAKE'**: Single source of truth, zero drift by design
2. **AUTO_REFRESH**: Event-driven sync for external catalogs
3. **Catalog-Linked Databases**: Auto-discover and sync all tables from external catalogs
4. **SYSTEM$GET_ICEBERG_TABLE_INFORMATION**: Expose metadata for external readers
