"""Read-only dashboard API for human-reviewed urban anomaly alerts."""
import json
import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["ALERT_TABLE"])
default_city = os.environ.get("DEFAULT_CITY", "New Delhi")


def _json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value)} is not JSON serializable")


def _response(status, payload):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(payload, default=_json_default),
    }


def handler(event, _context):
    params = event.get("queryStringParameters") or {}
    city = params.get("city", default_city)
    limit = min(int(params.get("limit", "50")), 100)

    result = table.query(
        KeyConditionExpression=Key("city").eq(city),
        ScanIndexForward=False,
        Limit=limit,
    )
    alerts = result.get("Items", [])

    summary = {
        "city": city,
        "total_alerts": len(alerts),
        "critical": sum(1 for alert in alerts if alert.get("severity") == "critical"),
        "high": sum(1 for alert in alerts if alert.get("severity") == "high"),
        "medium": sum(1 for alert in alerts if alert.get("severity") == "medium"),
        "low": sum(1 for alert in alerts if alert.get("severity") == "low"),
    }
    return _response(200, {"summary": summary, "alerts": alerts})
