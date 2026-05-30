"""Score fused sensor events for human-reviewed anomaly triage."""
import base64
import datetime as dt
import hashlib
import json
import os
from decimal import Decimal

import boto3

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
alert_table = dynamodb.Table(os.environ["ALERT_TABLE"])
gold_bucket = os.environ["GOLD_BUCKET"]
quarantine_bucket = os.environ["QUARANTINE_BUCKET"]
min_alert_score = Decimal(os.environ.get("MIN_ALERT_SCORE", "70"))


def _loads(record):
    raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
    return json.loads(raw, parse_float=Decimal)


def _score(payload):
    features = payload.get("features") or {}
    reasons = []
    score = Decimal("0")

    speed = Decimal(str(payload.get("speed_kph", 0) or 0))
    confidence = Decimal(str(payload.get("confidence", 0) or 0))

    if speed >= 130:
        score += Decimal("25")
        reasons.append("high_speed")
    if features.get("route_deviation_score", 0) >= 0.8:
        score += Decimal("20")
        reasons.append("route_deviation")
    if features.get("restricted_zone_proximity_m") is not None:
        proximity = Decimal(str(features["restricted_zone_proximity_m"]))
        if proximity <= 250:
            score += Decimal("20")
            reasons.append("near_restricted_zone")
    if features.get("multi_sensor_match_count", 0) >= 2:
        score += Decimal("15")
        reasons.append("multi_sensor_corroboration")
    if features.get("plate_visibility") == "obscured":
        score += Decimal("10")
        reasons.append("identifier_obscured")
    if payload.get("sensor_type") == "sigint_metadata":
        score += Decimal("5")
        reasons.append("metadata_only_signal")

    score = min(Decimal("100"), score * max(confidence, Decimal("0.25")))
    if score >= 85:
        severity = "critical"
    elif score >= 70:
        severity = "high"
    elif score >= 45:
        severity = "medium"
    else:
        severity = "low"
    return score.quantize(Decimal("0.01")), severity, reasons


def _coarse_coordinate(value):
    return Decimal(str(value)).quantize(Decimal("0.0001"))


def _alert_id(payload):
    raw = "||".join(
        str(payload.get(field, ""))
        for field in ["city", "track_id", "observed_at", "sensor_type"]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _put_json(bucket, key, payload):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def handler(event, _context):
    scored = 0
    alerted = 0
    quarantined = 0
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    for record in event.get("Records", []):
        payload = _loads(record)
        if not payload.get("authorization"):
            event_id = payload.get("event_id", _alert_id(payload))
            _put_json(
                quarantine_bucket,
                f"urban/scoring/rejected/{event_id}.json",
                {"error": "missing_authorization", "payload": payload},
            )
            quarantined += 1
            continue

        score, severity, reasons = _score(payload)
        scored += 1
        alert = {
            "city": payload.get("city", "unknown"),
            "alert_id": _alert_id(payload),
            "track_id": payload.get("track_id"),
            "created_at": now,
            "observed_at": payload.get("observed_at"),
            "severity": severity,
            "score": score,
            "lat": _coarse_coordinate(payload.get("lat", 0)),
            "lon": _coarse_coordinate(payload.get("lon", 0)),
            "sensor_type": payload.get("sensor_type"),
            "reason_codes": reasons,
            "status": "needs_human_review",
            "recommended_action": "review_context_and_validate_with_authorized_sources",
        }

        partition_date = dt.datetime.utcnow().strftime("%Y/%m/%d")
        _put_json(
            gold_bucket,
            f"urban/scored/city={alert['city']}/date={partition_date}/{alert['alert_id']}.json",
            alert,
        )

        if score >= min_alert_score:
            alert_table.put_item(Item=alert)
            alerted += 1

    return {"scored": scored, "alerted": alerted, "quarantined": quarantined}
