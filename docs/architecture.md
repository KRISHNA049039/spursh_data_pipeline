# MGM FX Pipeline + Spursh Approval System — AWS Architecture

## Stack 1: MgmFxPipelineStack (existing)

```
AR FX batch (S3) ──► BatchTrigger Lambda ──► Step Functions
                                                │
Intercompany FX ──► Kinesis ──► StreamIngestor  │
                                                ▼
                    Glue: Silver Cleanse (24-col, dedup, quarantine)
                                                │
                    Glue: Gold Shape (MEC-ready) │
                                                ▼
                    MEC Validator ──► OFA Booking ──► Audit Log
```

## Stack 2: SpurshApprovalStack (new)

```
Trigger (monthly/quarterly/yearly)
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  Step Functions — Spursh Pipeline                    │
│                                                      │
│  1. JE Generator Lambda                              │
│     └─ Creates JEs from AP, AR, IC ledgers           │
│     └─ All geos: US, EMEA, APAC, LATAM, etc.        │
│                                                      │
│  2. Trial Balance Validator Lambda                   │
│     └─ Dr=Cr per geo per ledger source               │
│     └─ Account existence in TB                       │
│     └─ Geo completeness                              │
│     └─ FAIL → quarantine + stop                      │
│                                                      │
│  3. MEC Validator Lambda                             │
│     └─ 24-column completeness                        │
│     └─ Dr=Cr per journal (zero tolerance)            │
│     └─ 7-segment COA validity                        │
│     └─ FAIL → quarantine + stop                      │
│                                                      │
│  4. Spursh Approval (WAIT_FOR_TASK_TOKEN)            │
│     └─ Validates primary ledger JEs                  │
│     └─ SNS → L7 manager email                        │
│     └─ Manager clicks APPROVE/REJECT via API GW      │
│     └─ REJECT → stop pipeline                        │
│                                                      │
│  5. FX Period Booking Lambda                         │
│     └─ MONTHLY: book as-is                           │
│     └─ QUARTERLY: net 3 months, book aggregated      │
│     └─ YEARLY: net 12 months + year-end entries      │
│     └─ Idempotent (audit table check)                │
│     └─ Posts to OFA, writes audit log                │
└─────────────────────────────────────────────────────┘
```


## Spursh Approval Flow (L7 Manager)

```
Spursh Approval Lambda
    │
    ├── Validates primary ledger (ledger_id ends with -PRIMARY)
    ├── Checks 24-col, no duplicate surrogate keys, payload_hash
    ├── Persists approval request to DynamoDB
    ├── Publishes SNS notification to L7 manager
    │
    └── Step Functions PAUSES (WAIT_FOR_TASK_TOKEN)
            │
            ▼
        L7 Manager receives email with APPROVE / REJECT links
            │
            ├── APPROVE → API Gateway → Callback Lambda
            │       └── SendTaskSuccess → pipeline resumes → Book to OFA
            │
            └── REJECT → API Gateway → Callback Lambda
                    └── SendTaskFailure → pipeline stops
```

## AWS Services (Spursh Stack)

| Service | Purpose |
|---------|---------|
| Lambda (JE Generator) | Create JEs from AP/AR/IC for all geos |
| Lambda (TB Validator) | Validate JEs against trial balance |
| Lambda (MEC Validator) | 24-col, Dr=Cr, 7-segment checks |
| Lambda (Spursh Approval) | Primary ledger validation + SNS notification |
| Lambda (Spursh Callback) | API GW handler for approve/reject |
| Lambda (FX Period Booking) | Monthly/quarterly/yearly OFA booking |
| Step Functions | Orchestration with human approval gate |
| API Gateway | L7 manager approval endpoint |
| SNS | Approval notification delivery |
| DynamoDB (JeTable) | JE storage and audit |
| DynamoDB (ApprovalTable) | Approval request tracking |
| DynamoDB (BookingAuditTable) | Booking audit trail |
| S3 (Gold) | JE batches and booked entries |
| S3 (Quarantine) | Failed validations |
| S3 (RefData) | Trial balance and COA reference data |

## Multi-Period FX Booking

| Period | Aggregation | Frequency |
|--------|-------------|-----------|
| MONTHLY | Pass-through — book individual JEs | Every month-end |
| QUARTERLY | Net by journal_id + geo + account | Every quarter-end |
| YEARLY | Net by geo + account, collapse all journals | Year-end |

## Supported Geos

US, EMEA, APAC, LATAM, CANADA, JAPAN, INDIA, UK

## Ledger Sources

AP (Accounts Payable), AR (Accounts Receivable), IC (Intercompany)
