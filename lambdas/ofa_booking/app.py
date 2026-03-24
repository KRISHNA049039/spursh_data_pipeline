"""
OFA Booking Lambda
Posts validated journal batches to Oracle Financials and records an
idempotent audit trail. Will not double-book a batch that already
has a POSTED record in the audit table.
"""
import datetime as dt
import json
import os
import uuid

import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["BOOKING_AUDIT_TABLE"])


def _already_booked(batch_id):
    """Check if this batch was already posted (idempotency guard)."""
    try:
        resp = table.query(
            IndexName="batch-status-index",
            KeyConditionExpression=(
                boto3.dynamodb.conditions.Key("journal_batch_id").eq(batch_id)
            ),
            FilterExpression=boto3.dynamodb.conditions.Attr("status").eq("POSTED"),
            Limit=1,
        )
        return len(resp.get("Items", [])) > 0
    except ClientError:
        # If the GSI doesn't exist yet, fall back to a scan (dev only)
        resp = table.scan(
            FilterExpression=(
                boto3.dynamodb.conditions.Attr("journal_batch_id").eq(batch_id)
                & boto3.dynamodb.conditions.Attr("status").eq("POSTED")
            ),
            Limit=1,
        )
        return len(resp.get("Items", [])) > 0


def _post_to_ofa(batch_id, validation_payload):
    """
    Integration point for Oracle Financials.
    Replace this stub with the real OFA API call or file-based interface.
    Returns an OFA journal reference on success.
    """
    # TODO: Replace with actual OFA REST/SOAP call
    ofa_journal_ref = f"OFA-{batch_id[:8]}-{uuid.uuid4().hex[:6].upper()}"
    return {
        "ofa_journal_ref": ofa_journal_ref,
        "status": "POSTED",
    }


def handler(event, _context):
    validation = event.get("validation", {})
    batch_id = event.get("batch_id", "UNKNOWN")

    # Hard stop — never book an invalid batch
    if not validation.get("is_valid"):
        raise ValueError(
            f"Batch {batch_id} failed MEC validation. OFA booking blocked."
        )

    # Idempotency — skip if already booked
    if _already_booked(batch_id):
        return {
            "booking_run_id": None,
            "journal_batch_id": batch_id,
            "status": "ALREADY_BOOKED",
            "message": f"Batch {batch_id} was already posted to OFA. Skipping.",
        }

    # Post to OFA
    booking_run_id = str(uuid.uuid4())
    ofa_result = _post_to_ofa(batch_id, validation)

    # Write audit log
    audit_item = {
        "booking_run_id": booking_run_id,
        "journal_batch_id": batch_id,
        "ofa_journal_ref": ofa_result["ofa_journal_ref"],
        "status": ofa_result["status"],
        "mec_checks_passed": True,
        "checks_run": validation.get("checks_run", 3),
        "checks_passed": validation.get("checks_passed", 3),
        "booked_at": dt.datetime.utcnow().isoformat(),
        "source_bucket": event.get("source_bucket", ""),
        "source_key": event.get("source_key", ""),
        "posted_by": "fx-pipeline-service",
    }
    table.put_item(Item=audit_item)

    return audit_item
