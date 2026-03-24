"""
FX Period Booking — Lambda
Books FX adjustments for monthly, quarterly, and yearly periods.
Wraps the OFA booking with period-aware logic:
  - MONTHLY:   books current month adjustments
  - QUARTERLY: aggregates 3 months, books net adjustments
  - YEARLY:    aggregates 12 months, books net adjustments + year-end entries
Idempotent — checks audit table before booking.
"""
import datetime as dt
import json
import os
import uuid
from collections import defaultdict
from decimal import Decimal, InvalidOperation

import boto3

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

BOOKING_AUDIT_TABLE = os.environ["BOOKING_AUDIT_TABLE"]
GOLD_BUCKET = os.environ["GOLD_BUCKET"]

audit_table = dynamodb.Table(BOOKING_AUDIT_TABLE)


def _dec(v):
    if v is None or v == "" or v == "null":
        return Decimal("0")
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _already_booked(batch_id):
    """Check audit table for existing POSTED record."""
    resp = audit_table.scan(
        FilterExpression=(
            boto3.dynamodb.conditions.Attr("journal_batch_id").eq(batch_id)
            & boto3.dynamodb.conditions.Attr("status").eq("POSTED")
        ),
        Limit=1,
    )
    return len(resp.get("Items", [])) > 0


def _aggregate_for_period(jes, period_type):
    """
    MONTHLY:   pass through as-is
    QUARTERLY: net by journal_id + geo + account
    YEARLY:    net by geo + account (collapse all journals)
    """
    if period_type == "MONTHLY":
        return jes

    group_key_fn = {
        "QUARTERLY": lambda je: (
            je.get("geo", ""), je.get("journal_id", ""),
            je.get("segment3_account", ""),
        ),
        "YEARLY": lambda je: (
            je.get("geo", ""), je.get("segment3_account", ""),
        ),
    }.get(period_type, lambda je: (je.get("journal_id", ""),))

    buckets = defaultdict(lambda: {
        "dr": Decimal("0"), "cr": Decimal("0"),
        "entered_dr": Decimal("0"), "entered_cr": Decimal("0"),
        "template": None,
    })

    for je in jes:
        key = group_key_fn(je)
        b = buckets[key]
        b["dr"] += _dec(je.get("accounted_dr"))
        b["cr"] += _dec(je.get("accounted_cr"))
        b["entered_dr"] += _dec(je.get("entered_dr"))
        b["entered_cr"] += _dec(je.get("entered_cr"))
        if b["template"] is None:
            b["template"] = dict(je)

    aggregated = []
    for idx, (key, b) in enumerate(buckets.items(), start=1):
        entry = dict(b["template"])
        entry["accounted_dr"] = str(b["dr"])
        entry["accounted_cr"] = str(b["cr"])
        entry["entered_dr"] = str(b["entered_dr"])
        entry["entered_cr"] = str(b["entered_cr"])
        entry["journal_line_num"] = idx
        entry["description"] = f"{period_type} FX adjustment — {entry.get('geo', '')}"
        aggregated.append(entry)

    return aggregated


def _post_to_ofa(batch_id, jes, period_type):
    """Stub for OFA API call. Replace with real integration."""
    ofa_ref = f"OFA-{period_type[:3]}-{batch_id[:8]}-{uuid.uuid4().hex[:6].upper()}"
    return {"ofa_journal_ref": ofa_ref, "status": "POSTED", "line_count": len(jes)}


def handler(event, _context):
    """
    Expected event:
    {
      "batch_id": "...",
      "period_type": "MONTHLY" | "QUARTERLY" | "YEARLY",
      "journal_entries": [...],
      "approval": { "decision": "APPROVED", ... }
    }
    """
    batch_id = event.get("batch_id", "UNKNOWN")
    period_type = event.get("period_type", "MONTHLY")
    jes = event.get("journal_entries", [])
    approval = event.get("approval", {})

    # Must be approved
    if approval.get("decision") != "APPROVED":
        raise ValueError(
            f"Batch {batch_id} not approved. Cannot book to OFA."
        )

    # Idempotency
    if _already_booked(batch_id):
        return {
            "batch_id": batch_id,
            "status": "ALREADY_BOOKED",
            "period_type": period_type,
        }

    # Aggregate based on period type
    booking_jes = _aggregate_for_period(jes, period_type)

    # Post to OFA
    ofa_result = _post_to_ofa(batch_id, booking_jes, period_type)

    # Audit log
    booking_run_id = str(uuid.uuid4())
    audit_item = {
        "booking_run_id": booking_run_id,
        "journal_batch_id": batch_id,
        "period_type": period_type,
        "ofa_journal_ref": ofa_result["ofa_journal_ref"],
        "status": ofa_result["status"],
        "line_count": ofa_result["line_count"],
        "booked_at": dt.datetime.utcnow().isoformat(),
        "approved_by": approval.get("approved_by", ""),
        "posted_by": "spursh-pipeline-service",
    }
    audit_table.put_item(Item=audit_item)

    # Persist booked JEs to Gold for reconciliation
    gold_key = f"booked/{period_type.lower()}/{batch_id}.json"
    s3.put_object(
        Bucket=GOLD_BUCKET,
        Key=gold_key,
        Body=json.dumps(booking_jes, default=str).encode("utf-8"),
        ContentType="application/json",
    )

    return {
        "batch_id": batch_id,
        "booking_run_id": booking_run_id,
        "period_type": period_type,
        "ofa_journal_ref": ofa_result["ofa_journal_ref"],
        "status": "POSTED",
        "line_count": ofa_result["line_count"],
    }
