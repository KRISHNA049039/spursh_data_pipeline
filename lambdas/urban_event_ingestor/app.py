"""Normalize authorized synthetic/operational sensor events into Bronze."""
import base64
import datetime as dt
import hashlib
import json
import os
from decimal import Decimal

import boto3

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["IDEMPOTENCY_TABLE"])
bronze_bucket = os.environ["BRONZE_BUCKET"]
quarantine_bucket = os.environ["QUARANTINE_BUCKET"]

ALLOWED_SENSOR_TYPES = {"radar", "sigint_metadata", "aircraft_iot", "vehicle_iot"}
REQUIRED_AUTH_FIELDS = {"authority", "case_id", "retention_policy"}


def _loads_kinesis_record(record):
    raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
    return json.loads(raw, parse_float=Decimal)


def _event_id(payload):
    if payload.get("event_id"):
        return str(payload["event_id"])
    raw = "||".join(
        str(payload.get(field, ""))
        for field in ["sensor_type", "asset_id", "observed_at", "lat", "lon"]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _validate(payload):
    errors = []
    sensor_type = payload.get("sensor_type")
    if sensor_type not in ALLOWED_SENSOR_TYPES:
        errors.append("unsupported_sensor_type")

    authorization = payload.get("authorization") or {}
    missing_auth = sorted(REQUIRED_AUTH_FIELDS - set(authorization))
    if missing_auth:
        errors.append(f"missing_authorization:{','.join(missing_auth)}")

    for field in ["city", "observed_at", "lat", "lon"]:
        if payload.get(field) in [None, ""]:
            errors.append(f"missing_{field}")

    try:
        lat = Decimal(str(payload.get("lat")))
        lon = Decimal(str(payload.get("lon")))
        if not Decimal("-90") <= lat <= Decimal("90"):
            errors.append("lat_out_of_range")
        if not Decimal("-180") <= lon <= Decimal("180"):
            errors.append("lon_out_of_range")
    except Exception:
        errors.append("invalid_coordinates")

    return errors


def _normalize(payload):
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    event_id = _event_id(payload)
    normalized = {
        "event_id": event_id,
        "sensor_type": payload.get("sensor_type"),
        "city": str(payload.get("city", "")).strip(),
        "observed_at": payload.get("observed_at"),
        "received_at": now,
        "asset_id_hash": hashlib.sha256(
            str(payload.get("asset_id", "unknown")).encode("utf-8")
        ).hexdigest(),
        "track_id": str(payload.get("track_id", event_id[:16])),
        "lat": payload.get("lat"),
        "lon": payload.get("lon"),
        "speed_kph": payload.get("speed_kph"),
        "heading_deg": payload.get("heading_deg"),
        "confidence": payload.get("confidence", Decimal("0.5")),
        "features": payload.get("features", {}),
        "authorization": payload.get("authorization", {}),
        "source_schema_version": payload.get("schema_version", "1.0"),
    }
    normalized["payload_hash"] = hashlib.sha256(
        json.dumps(normalized, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return normalized


def _put_json(bucket, key, payload):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def handler(event, _context):
    accepted = 0
    skipped = 0
    quarantined = 0

    for record in event.get("Records", []):
        payload = _loads_kinesis_record(record)
        errors = _validate(payload)
        event_id = _event_id(payload)
        partition_date = dt.datetime.utcnow().strftime("%Y/%m/%d")

        if errors:
            _put_json(
                quarantine_bucket,
                f"urban/events/{partition_date}/{event_id}.json",
                {"event_id": event_id, "errors": errors, "payload": payload},
            )
            quarantined += 1
            continue

        stage = "bronze"
        existing = table.get_item(
            Key={"event_id": event_id, "stage": stage},
            ConsistentRead=True,
        )
        if "Item" in existing:
            skipped += 1
            continue

        normalized = _normalize(payload)
        object_key = (
            f"urban/bronze/city={normalized['city']}/date={partition_date}/"
            f"{event_id}.json"
        )
        _put_json(bronze_bucket, object_key, normalized)
        table.put_item(
            Item={
                "event_id": event_id,
                "stage": stage,
                "object_key": object_key,
                "payload_hash": normalized["payload_hash"],
                "processed_at": normalized["received_at"],
            }
        )
        accepted += 1

    return {"accepted": accepted, "skipped": skipped, "quarantined": quarantined}
