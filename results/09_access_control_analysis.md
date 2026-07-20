# Challenge 9: Fragmented Access Control - Analysis Results

**Date:** 2026-07-20
**Table:** `employee_compensation` (1,000 rows, Iceberg table with PII)

---

## The Problem

In open-source Iceberg, managing row and column-level security requires:
- External tools (Apache Ranger, AWS Lake Formation)
- Different engines enforce different security models
- Policies don't travel with the data
- Row-level filtering requires view-based workarounds
- No column masking in the Iceberg format specification

---

## Snowflake's Mitigation: Native Governance on Iceberg

### Policies Applied to the Iceberg Table

| Policy | Type | Target | Rule |
|--------|------|--------|------|
| `region_filter_policy` | ROW ACCESS POLICY | `region` column | Analysts see only `us-east-1` |
| `ssn_mask` | MASKING POLICY | `ssn` column | Analysts see `***-**-XXXX` |
| `salary_mask` | MASKING POLICY | `salary` column | Analysts see `NULL` |

### Verification: Policies Are ACTIVE on ICEBERG_TABLE

```
| POLICY_NAME            | POLICY_KIND        | REF_ENTITY_DOMAIN | POLICY_STATUS |
|------------------------|--------------------|-------------------|---------------|
| SALARY_MASK            | MASKING_POLICY     | ICEBERG_TABLE     | ACTIVE        |
| SSN_MASK               | MASKING_POLICY     | ICEBERG_TABLE     | ACTIVE        |
| REGION_FILTER_POLICY   | ROW_ACCESS_POLICY  | ICEBERG_TABLE     | ACTIVE        |
```

Note: `REF_ENTITY_DOMAIN = ICEBERG_TABLE` confirms these are applied directly to the Iceberg table, not a view wrapper.

---

## Access by Role

### ACCOUNTADMIN / Engineer (Full Access)
```
| EMPLOYEE_ID | FULL_NAME  | REGION    | SALARY     | SSN         | DEPARTMENT  |
|-------------|------------|-----------|------------|-------------|-------------|
| 1           | Employee_1 | us-west-2 | 135,522.34 | 694-67-3903 | Sales       |
| 2           | Employee_2 | us-east-1 | 105,987.41 | 972-97-8496 | Sales       |
| 3           | Employee_3 | eu-west-1 | 120,453.86 | 137-58-8693 | Engineering |
```
All regions visible, salary in clear, SSN in clear.

### Analyst Role (Restricted by Policies)
```
| EMPLOYEE_ID | FULL_NAME  | REGION    | SALARY | SSN          | DEPARTMENT  |
|-------------|------------|-----------|--------|--------------|-------------|
| 2           | Employee_2 | us-east-1 | NULL   | ***-**-8496  | Sales       |
| 10          | Employee_10| us-east-1 | NULL   | ***-**-2024  | HR          |
```
- Only `us-east-1` rows visible (row access policy)
- Salary is NULL (masking policy)
- SSN shows only last 4 digits (masking policy)

---

## Key Capabilities Demonstrated

1. **Row Access Policy on Iceberg** — filters rows based on `CURRENT_ROLE()`
2. **Column Masking on Iceberg** — masks SSN and salary per role
3. **Platform-Level Enforcement** — cannot be bypassed (not view-level)
4. **Same Syntax as Native Tables** — zero learning curve
5. **Policy Portability** — same policies work on native AND Iceberg tables

---

## OSS Iceberg vs Snowflake Comparison

| Aspect | OSS Iceberg | Snowflake |
|--------|------------|-----------|
| Row-level security | Apache Ranger (separate tool) | Native ROW ACCESS POLICY |
| Column masking | Not available in Iceberg spec | Native MASKING POLICY |
| Enforcement level | Engine-dependent (bypassable) | Platform-level (cannot bypass) |
| Audit trail | Fragmented across engines | Unified ACCESS_HISTORY |
| Policy management | Per-engine configuration | Single policy, all engines |
| Works across engines | Only for engines with Ranger plugin | Always enforced in Snowflake |
| Additional infrastructure | Ranger + Solr + ZooKeeper | None |

---

## Conclusion

Snowflake provides native, platform-enforced governance on Iceberg tables:
1. **ROW ACCESS POLICY** works directly on Iceberg tables (filters by role/context)
2. **MASKING POLICY** masks sensitive columns per role
3. **Cannot be bypassed** — enforced at platform level, not engine level
4. **Zero additional tools** — no Ranger, no Lake Formation, no view wrappers
5. **Same policies for native AND Iceberg** — unified governance model
