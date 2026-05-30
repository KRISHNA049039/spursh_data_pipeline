# MGM FX Data Pipeline on AWS

This repository contains a starter AWS implementation of the MGM Accounting FX pipeline described in the architecture reference:

- `AR FX` batch ingestion from S3
- `Intercompany FX` stream ingestion from Kinesis
- Medallion lake zones in S3: Bronze, Silver, Gold, Quarantine
- Idempotent processing backed by deterministic surrogate keys
- MEC validation gate before Oracle Financials (OFA) booking
- Booking audit persistence in DynamoDB

## Architecture

The stack provisions these AWS services:

- `Amazon S3` for Bronze, Silver, Gold, Quarantine, and Glue script assets
- `AWS Kinesis Data Streams` for Intercompany FX events
- `AWS Lambda` for batch orchestration, stream normalization, MEC validation, and OFA booking
- `AWS Step Functions` to orchestrate the AR FX batch path
- `AWS Glue` for Silver cleansing and Gold journal shaping
- `Amazon DynamoDB` for idempotency state and booking audit logs
- `AWS Glue Data Catalog` database for lake datasets

## Repository Layout

```text
.
|-- app.py
|-- cdk.json
|-- requirements.txt
|-- fx_pipeline/
|   `-- fx_pipeline_stack.py
|-- lambdas/
|   |-- batch_trigger/
|   |   `-- app.py
|   |-- stream_ingestor/
|   |   `-- app.py
|   |-- mec_validator/
|   |   `-- app.py
|   `-- ofa_booking/
|       `-- app.py
|-- glue/
|   |-- cleanse_job.py
|   `-- gold_job.py
`-- docs/
    `-- architecture.md
```

## Data Flow

### AR FX Batch

1. Upstream drops an AR FX file into the Bronze bucket under `landing/ar_fx/`.
2. S3 triggers `batch_trigger` Lambda.
3. Lambda starts the Step Functions state machine.
4. State machine runs the Silver cleanse Glue job.
5. State machine runs the Gold shaping Glue job.
6. MEC validation Lambda gates the batch.
7. If validation passes, OFA booking Lambda records a booking audit entry and is ready to call the OFA API.

### Intercompany FX Stream

1. Upstream publishes events to the Kinesis stream.
2. `stream_ingestor` Lambda consumes the records.
3. Lambda normalizes the payload, computes the surrogate key, checks idempotency in DynamoDB, and lands accepted events in Bronze.
4. The stream path can later be extended to invoke Silver/Gold transforms on a micro-batch cadence.

## Deploy

### Prerequisites

- Python 3.11+
- AWS credentials configured locally
- CDK bootstrap already completed in the target account and region

### Install

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Synthesize / Deploy

```bash
cdk synth
cdk deploy
```

## Assumptions

- The OFA integration is represented as a Lambda stub so the real Oracle endpoint and auth model can be plugged in later.
- Glue jobs assume JSON or CSV source files for the batch path.
- MEC validation currently enforces canonical columns, account segment presence, and debit-credit balancing from journal lines supplied in the batch payload. For non-JSON source files, extend the workflow to read Gold output or generate a validation manifest before booking.
- The stream ingestion path lands normalized events and idempotency metadata first; production teams would usually add Firehose, Glue streaming, or a micro-batch Step Functions path after this starter.

## Next Steps

- Replace the OFA booking stub with the production Oracle Financials adapter
- Add Lake Formation permissions for Finance and Audit personas
- Register Glue crawlers or explicit tables for Silver and Gold datasets
- Add CI/CD and environment-specific CDK stages
- Add unit tests plus contract tests around canonical schema validation

## Urban Anomaly Intelligence Pipeline

This repository also includes a separate responsible sensor-fusion starter for
authorized public-safety anomaly triage across capital cities:

- `urban_anomaly_pipeline/urban_anomaly_stack.py` provisions Kinesis, Lambda,
  S3, DynamoDB, API Gateway, a Glue database, and static dashboard assets.
- `lambdas/urban_event_ingestor/` validates and minimizes incoming sensor
  events, requiring authorization metadata before Bronze landing.
- `lambdas/urban_anomaly_scorer/` creates explainable anomaly alerts with
  reason codes and a human-review status.
- `lambdas/urban_dashboard_api/` exposes read-only alert data for dashboards.
- `dashboard/urban-anomaly/` is a static dashboard that works with sample data
  locally or a deployed API via `?api=<api-url>`.
- `docs/urban_anomaly_srs.md` contains the SRS.
- `docs/urban_anomaly_architecture.md` contains the architecture notes.

The implementation is intentionally scoped for synthetic demos and authorized
operations. It does not automate enforcement decisions or provide tools for
unauthorized surveillance, interception, or targeting.
