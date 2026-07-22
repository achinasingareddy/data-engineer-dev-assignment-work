# Data Quality Rules

| Rule ID | Severity | Condition | Action |
|----------|----------|-----------|--------|
| DQ-Q1 | Critical | Invalid longitude or latitude | Quarantine |
| DQ-W1 | Warning | City is NULL, blank or UNKNOWN | Publish with WARNING |

---

## Quarantine Output

Reason Code

INVALID_COORDINATES

Destination

/Volumes/dev_engineering_catalog/dev_engineering_schema/dev_engineering_volume/quarantine/<run_id>/

---

## Warning Output

dq_status = WARNING

dq_warnings =

MISSING_OR_UNKNOWN_CITY

Destination

Gold

---

## Metrics

Every execution logs

Rows In

Rows OK

Rows Warning

Rows Quarantined

Execution Time
