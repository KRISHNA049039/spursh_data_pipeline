"""
Stream Ingestor Lambda — Intercompany FX
Consumes Kinesis events, normalises to the canonical 24-column schema,
computes a deterministic surrogate key, and lands records in Bronze.
DynamoDB provides idempotency — duplicate surrogate keys are skipped.
"""
import base64
import datetime as dt
import hashlib
import json
import os

import boto3

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["IDEMPOTENCY_TABLE"])
bronze_bucket = os.environ["BRONZE_BUCKET"]


def build_surrogate_key(record):
    """Deterministic MD5 from business-identity fields (matches Glue logic)."""
    raw = "||".join([
        record.get("source_system", "ICO_FX"),
        str(record.get("journal_id", "")),
        str(record.get("journal_line_num", record.get("ledger_line_num", ""))),
        str(record.get("effective_date", "")),
        str(record.get("entered_dr", record.get("amount", ""))),
    ])
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def build_payload_hash(record):
    """SHA-256 of business payload columns for change detection."""
    payload_fields = [
        "journal_id", "journal_line_num", "ledger_id", "effective_date",
        "period_name", "currency_code", "entered_dr", "entered_cr",
        "accounted_dr", "accounted_cr", "fx_rate",
        "segment1_entity", "segment2_cost_center", "segment3_account",
        "segment4_project", "segment5_intercompany", "segment6_product",
        "segment7_spare", "description", "batch_id",
    ]
    raw = "||".join(str(record.get(f, "")) for f in payload_fields)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _normalise(payload):
    """Map upstream Kinesis event to the canonical 24-column schema."""
    now = dt.datetime.utcnow()
    effective = payload.get("effective_date", "")

    # Derive period_name from effective_date (e.g. "JAN-2024")
    period_name = ""
    if effective:
        try:
            d = dt.datetime.strptime(effective[:10], "%Y-%m-%d")
            period_name = d.strftime("%b-%Y").upper()
        except ValueError:
            period_name = ""

    # Map upstream segment fields — support both flat and nested formats
    segments = payload.get("account_segments", {})

    record = {
        "source_system":          "ICO_FX",
        "journal_id":             payload.get("journal_id"),
        "journal_line_num":       payload.get("journal_line_num",
                                              payload.get("ledger_line_num")),
        "ledger_id":              payload.get("ledger_id"),
        "effective_date":         effective,
        "period_name":            period_name,
        "currency_code":          payload.get("currency_code"),
        "entered_dr":             payload.get("entered_dr"),
        "entered_cr":             payload.get("entered_cr"),
        "accounted_dr":           payload.get("accounted_dr"),
        "accounted_cr":           payload.get("accounted_cr"),
        "fx_rate":                payload.get("fx_rate"),
        "segment1_entity":        payload.get("segment1_entity",
                                              segments.get("entity",
                                              payload.get("legal_entity"))),
        "segment2_cost_center":   payload.get("segment2_cost_center",
                                              segments.get("cost_center")),
        "segment3_account":       payload.get("segment3_account",
                                              segments.get("account")),
        "segment4_project":       payload.get("segment4_project",
                                              segments.get("project")),
        "segment5_intercompany":  payload.get("segment5_intercompany",
                                              segments.get("intercompany")),
        "segment6_product":       payload.get("segment6_product",
                                              segments.get("product")),
        "segment7_spare":         payload.get("segment7_spare",
                                              segments.get("spare", "000")),
        "description":            payload.get("description", ""),
        "batch_id":               f"ico-stream-{now.strftime('%Y%m%d')}",
        "load_timestamp":         now.isoformat(),
    }

    record["surrogate_key"] = build_surrogate_key(record)
    record["payload_hash"] = build_payload_hash(record)
    return record


def handler(event, _context):
    accepted = 0
    skipped = 0

    for kinesis_record in event.get("Records", []):
        raw = base64.b64decode(kinesis_record["kinesis"]["data"]).decode("utf-8")
        payload = json.loads(raw)
        normalized = _normalise(payload)

        surrogate_key = normalized["surrogate_key"]
        stage = "bronze"

        # Idempotency check via DynamoDB
        existing = table.get_item(
            Key={"surrogate_key": surrogate_key, "stage": stage},
            ConsistentRead=True,
        )
        if "Item" in existing:
            skipped += 1
            continue

        # Land in Bronze
        partition_date = dt.datetime.utcnow().strftime("%Y/%m/%d")
        object_key = f"stream/intercompany_fx/{partition_date}/{surrogate_key}.json"

        s3.put_object(
            Bucket=bronze_bucket,
            Key=object_key,
            Body=json.dumps(normalized).encode("utf-8"),
            ContentType="application/json",
        )

        # Record in idempotency table
        table.put_item(
            Item={
                "surrogate_key": surrogate_key,
                "stage": stage,
                "source_system": "ICO_FX",
                "object_key": object_key,
                "payload_hash": normalized["payload_hash"],
                "processed_at": dt.datetime.utcnow().isoformat(),
            }
        )
        accepted += 1

    return {"accepted": accepted, "skipped": skipped}
