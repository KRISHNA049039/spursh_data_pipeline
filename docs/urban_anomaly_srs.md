# Software Requirements Specification: Urban Anomaly Intelligence Pipeline

## 1. Purpose

This project provides a responsible sensor-fusion data pipeline and dashboard for authorized public-safety or defense operations. It ingests radar detections, authorized signal-derived metadata, aircraft IoT telemetry, and vehicle IoT telemetry across capital cities, then produces explainable anomaly alerts with coarse map coordinates for human review.

The system must not make automated enforcement decisions. It must support lawful authorization, minimization, auditability, and analyst accountability.

## 2. Scope

Included:

- Multi-source event ingestion through a streaming interface.
- Bronze and Gold lake storage for normalized events and scored alerts.
- Quarantine storage for invalid, unauthorized, or malformed records.
- Deterministic idempotency checks.
- Explainable anomaly scoring with reason codes.
- Read-only dashboard API and static dashboard.
- SRS, architecture, sample synthetic data, and AWS CDK infrastructure.

Excluded:

- Real interception, decryption, or exploitation of communications.
- Automated pursuit, targeting, detention, or weapon-system tasking.
- Bypassing legal process, consent, agency policy, or data-sharing agreements.
- Face recognition, protected-class inference, or individual profiling outside a validated lawful basis.

## 3. Users

- Operations analyst: reviews alerts, verifies context, and escalates through approved procedures.
- Data engineer: maintains schemas, pipelines, quality checks, and data retention.
- System administrator: deploys infrastructure and manages access controls.
- Compliance auditor: reviews authorization, data access, alert decisions, and retention.

## 4. Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-01 | Ingest JSON events from radar, authorized signal metadata, aircraft IoT, and vehicle IoT into Kinesis. | High |
| FR-02 | Validate sensor type, city, observation time, coordinates, and authorization fields. | High |
| FR-03 | Quarantine events that are malformed or missing authorization metadata. | High |
| FR-04 | Hash asset identifiers before landing normalized data. | High |
| FR-05 | Deduplicate events using deterministic event IDs and an idempotency table. | High |
| FR-06 | Store normalized events in a Bronze data lake partitioned by city and date. | High |
| FR-07 | Score anomalies using configurable features and explainable reason codes. | High |
| FR-08 | Store scored alert records in a Gold bucket and high-priority alerts in DynamoDB. | High |
| FR-09 | Provide a read-only dashboard API filtered by city and limit. | Medium |
| FR-10 | Render a dashboard with summary counts, schematic map markers, alert severity, score, reason codes, and review status. | Medium |
| FR-11 | Provide synthetic sample data for local demos. | Medium |
| FR-12 | Keep all alert records in `needs_human_review` status until a downstream authorized workflow updates them. | High |

## 5. Nonfunctional Requirements

| ID | Requirement |
|----|-------------|
| NFR-01 | All buckets enforce SSL and versioning where appropriate. |
| NFR-02 | Dashboard API must be read-only and return only minimized alert fields. |
| NFR-03 | Alert coordinates must be rounded to reduce precision in broad dashboards. |
| NFR-04 | Processing must support at least 100 events per Lambda batch in the starter configuration. |
| NFR-05 | Every alert must include reason codes and score for analyst explainability. |
| NFR-06 | Deployment must be reproducible through AWS CDK. |
| NFR-07 | Retention must be configurable by environment before production use. |
| NFR-08 | Production deployments must add IAM least privilege, encryption keys, private dashboard hosting, and centralized audit logs. |

## 6. Data Contract

Minimum input event:

```json
{
  "event_id": "demo-001",
  "schema_version": "1.0",
  "sensor_type": "vehicle_iot",
  "city": "New Delhi",
  "observed_at": "2026-05-30T10:32:00Z",
  "asset_id": "vehicle-alpha",
  "track_id": "track-91a",
  "lat": 28.6139,
  "lon": 77.209,
  "speed_kph": 142,
  "heading_deg": 84,
  "confidence": 0.96,
  "features": {
    "route_deviation_score": 0.91,
    "restricted_zone_proximity_m": 180,
    "multi_sensor_match_count": 3,
    "plate_visibility": "obscured"
  },
  "authorization": {
    "authority": "DEMO_PUBLIC_SAFETY",
    "case_id": "SIM-2026-001",
    "retention_policy": "demo-30-days"
  }
}
```

## 7. Anomaly Scoring

The starter scorer uses transparent heuristics:

- High speed.
- Route deviation.
- Proximity to restricted zones.
- Multi-sensor corroboration.
- Obscured identifiers.
- Metadata-only signal context.

The first production model should be evaluated against labeled historical data and must include bias, false-positive, false-negative, drift, and operator override analysis before deployment.

## 8. Safety, Privacy, and Governance

- Require authorization metadata on every event.
- Hash asset identifiers before storage.
- Show only coarse coordinates in the dashboard.
- Keep raw sensitive payloads out of the dashboard API.
- Require human review before operational escalation.
- Log access, changes, and review decisions in production.
- Add retention enforcement before real data use.
- Conduct legal, civil-liberties, cybersecurity, and model-risk reviews before deployment.

## 9. Acceptance Criteria

- CDK app synthesizes with the new `UrbanAnomalyStack`.
- Invalid or unauthorized records are quarantined.
- Valid records land in Bronze with hashed asset identifiers.
- High-scoring events create DynamoDB alert records.
- Dashboard loads sample data locally without a backend.
- Dashboard can query the deployed API by passing `?api=<api-url>`.
