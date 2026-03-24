# MGM FX Pipeline — AWS Architecture

## Pipeline Flow

```
AR FX batch (S3) ──► S3 notification ──► BatchTrigger Lambda ──► Step Functions
                                                                    │
Intercompany FX ──► Kinesis ──► StreamIngestor Lambda ──► Bronze    │
                                                                    │
                    ┌───────────────────────────────────────────────┘
                    ▼
              Glue: Silver Cleanse (24-col schema, dedup, quarantine)
                    │
                    ▼
              Glue: Gold Shape (MEC-ready facts, full 24-col preserved)
                    │
                    ▼
              Lambda: MEC Validator
                ├── Check 1: 24-column completeness
                ├── Check 2: Dr = Cr per journal (zero tolerance)
                └── Check 3: 7-segment COA validity
                    │
                    ├── FAIL ──► Quarantine + alert
                    │
                    └── PASS ──► Lambda: OFA Booking ──► Audit log
```

## AWS Services

| Layer | Service | Purpose |
|-------|---------|---------|
| Ingestion (batch) | S3 + Lambda (BatchTrigger) | Detect AR FX file drops, start Step Functions |
| Ingestion (stream) | Kinesis + Lambda (StreamIngestor) | Consume Intercompany FX events, land in Bronze |
| Cleansing | Glue ETL (Silver Cleanse) | 24-col normalisation, surrogate key, dedup, quarantine |
| Data Lake | S3 (Bronze / Silver / Gold / Quarantine) | Medallion architecture |
| MEC Validation | Lambda (MEC Validator) | 24-col check, Dr=Cr balance, 7-segment COA |
| OFA Booking | Lambda (OFA Booking) | Idempotent posting to Oracle Financials + audit log |
| Orchestration | Step Functions | AR FX batch pipeline: Cleanse → Gold → MEC → OFA |
| Idempotency | DynamoDB | Surrogate key dedup for stream path |
| Audit | DynamoDB (BookingAuditTable) | Booking run history, OFA journal refs |
| Reference Data | S3 (RefDataBucket) | COA segment validation reference tables |

## Idempotency Strategy

- Surrogate key = `MD5(source_system || journal_id || journal_line_num || effective_date || entered_dr)`
- Payload hash = `SHA-256(cols 3-22)` for change detection
- Stream path: DynamoDB rejects duplicate `(surrogate_key, stage)` before S3 write
- Batch path: Glue deduplicates on surrogate_key before Silver write
- OFA path: Audit table prevents double-booking of the same batch_id

## MEC Validation Gate (3 checks, all must pass)

1. **24-column completeness** — all columns present, non-nullable fields populated
2. **Dr = Cr balance** — `sum(accounted_dr) == sum(accounted_cr)` per journal_id, zero tolerance
3. **7-segment COA validity** — each segment value validated against reference data

Failed batches are quarantined to S3 and never reach OFA.

## Data Lake Tiers

| Tier | Bucket | Contents |
|------|--------|----------|
| Bronze | BronzeBucket | Raw immutable copies of source files and stream events |
| Silver | SilverBucket | Cleansed, typed, deduplicated 24-column canonical schema |
| Gold | GoldBucket | MEC-ready facts partitioned by batch_id |
| Quarantine | QuarantineBucket | Failed rows and rejected MEC batches |
