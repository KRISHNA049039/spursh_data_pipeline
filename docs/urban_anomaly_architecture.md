# Urban Anomaly Intelligence Architecture

```text
Authorized sensor feeds
  radar | authorized signal metadata | aircraft IoT | vehicle IoT
        |
        v
Kinesis: UrbanSensorEventStream
        |
        +--> UrbanEventIngestor Lambda
        |      - validate schema and authorization
        |      - hash asset identifiers
        |      - idempotency check
        |      - write Bronze or Quarantine
        |
        +--> UrbanAnomalyScorer Lambda
               - transparent anomaly scoring
               - reason codes
               - write Gold
               - write high-priority alerts to DynamoDB
                       |
                       v
              Dashboard API Lambda + API Gateway
                       |
                       v
              Static Urban Anomaly Dashboard
```

## AWS Resources

| Resource | Purpose |
|----------|---------|
| Kinesis stream | Streaming event entry point. |
| Bronze S3 bucket | Normalized minimized event lake. |
| Gold S3 bucket | Scored alert lake. |
| Quarantine S3 bucket | Invalid or unauthorized records. |
| DynamoDB idempotency table | Event deduplication. |
| DynamoDB alert table | Read-optimized alert queue. |
| Lambda ingestor | Validation, minimization, hashing, Bronze landing. |
| Lambda scorer | Explainable anomaly scoring and alert creation. |
| Lambda dashboard API | Read-only alert API. |
| API Gateway | HTTPS dashboard API endpoint. |
| Static dashboard bucket | Deployed dashboard assets. |
| Glue database | Lake catalog namespace for future crawlers/tables. |

## Production Hardening

- Replace demo heuristics with validated models and model monitoring.
- Add private VPC endpoints where needed.
- Use customer-managed KMS keys for S3, DynamoDB, and logs.
- Put the dashboard behind identity-aware access control.
- Add CloudTrail, DynamoDB streams, and immutable audit storage.
- Add Lake Formation permissions and column-level controls.
- Add schema registry and contract tests for each sensor source.
- Add data retention workflows by city, case, and authority.
