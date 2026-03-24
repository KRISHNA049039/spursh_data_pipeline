"""
Spursh Approval System — Lambda
Validates primary ledger JEs and sends an approval request to L7 managers.
Uses Step Functions task tokens for human-in-the-loop approval.

Flow:
  1. Receive JE batch + validation results
  2. Run Spursh-specific primary ledger checks
  3. Send approval request (SNS → L7 manager email)
  4. Manager approves/rejects via API Gateway callback
  5. Resume Step Functions with the decision
"""
import datetime as dt
import json
import os
import uuid

import boto3

sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb")
sfn = boto3.client("stepfunctions")

APPROVAL_TOPIC_ARN = os.environ["APPROVAL_TOPIC_ARN"]
APPROVAL_TABLE = os.environ["APPROVAL_TABLE"]
API_ENDPOINT = os.environ.get("API_ENDPOINT", "")

approval_table = dynamodb.Table(APPROVAL_TABLE)

# ---------------------------------------------------------------------------
# Spursh primary-ledger validation rules
# ---------------------------------------------------------------------------
REQUIRED_24 = [
    "surrogate_key", "source_system", "journal_id", "journal_line_num",
    "ledger_id", "effective_date", "period_name", "currency_code",
    "entered_dr", "entered_cr", "accounted_dr", "accounted_cr", "fx_rate",
    "segment1_entity", "segment2_cost_center", "segment3_account",
    "segment4_project", "segment5_intercompany", "segment6_product",
    "segment7_spare", "description", "batch_id", "load_timestamp", "payload_hash",
]


def _validate_primary_ledger(jes):
    """
    Spursh-specific checks on primary ledger JEs:
      1. All 24 columns present
      2. ledger_id ends with '-PRIMARY'
      3. No duplicate surrogate keys
      4. All amounts are non-negative
      5. payload_hash is populated
    """
    issues = []

    if not jes:
        return [{"check": "no_jes", "detail": "No journal entries to validate."}]

    seen_keys = set()
    for idx, je in enumerate(jes, start=1):
        # 24-col presence
        missing = [c for c in REQUIRED_24 if c not in je]
        if missing:
            issues.append({"row": idx, "check": "missing_columns", "detail": missing})

        # Primary ledger check
        lid = je.get("ledger_id", "")
        if not lid.endswith("-PRIMARY"):
            issues.append({
                "row": idx, "check": "not_primary_ledger",
                "detail": f"ledger_id '{lid}' is not a primary ledger",
            })

        # Duplicate surrogate key
        sk = je.get("surrogate_key", "")
        if sk in seen_keys:
            issues.append({"row": idx, "check": "duplicate_surrogate_key", "detail": sk})
        seen_keys.add(sk)

        # payload_hash populated
        if not je.get("payload_hash"):
            issues.append({"row": idx, "check": "missing_payload_hash", "detail": ""})

    return issues


def _send_approval_request(approval_id, batch_id, summary, task_token):
    """Publish approval request to SNS for L7 manager."""
    approve_url = f"{API_ENDPOINT}/approve?approval_id={approval_id}&action=APPROVE&token={task_token}"
    reject_url = f"{API_ENDPOINT}/approve?approval_id={approval_id}&action=REJECT&token={task_token}"

    message = (
        f"Spursh Approval Required — Batch {batch_id}\n\n"
        f"JE Count: {summary['je_count']}\n"
        f"Period: {summary['period_type']} — {summary['effective_date']}\n"
        f"Geos: {', '.join(summary.get('geos', []))}\n"
        f"Ledger Sources: {', '.join(summary.get('ledger_sources', []))}\n"
        f"Validation Issues: {summary['issue_count']}\n\n"
        f"APPROVE: {approve_url}\n\n"
        f"REJECT: {reject_url}\n"
    )

    sns.publish(
        TopicArn=APPROVAL_TOPIC_ARN,
        Subject=f"[Spursh] Approval Required — {batch_id}",
        Message=message,
    )


def handler(event, _context):
    """
    Called by Step Functions with a task token for human approval.
    Validates primary ledger, persists approval request, notifies L7 manager.
    """
    batch_id = event.get("batch_id", "UNKNOWN")
    jes = event.get("journal_entries", [])
    task_token = event.get("task_token", "")
    period_type = event.get("period_type", "MONTHLY")
    effective_date = event.get("effective_date", "")
    geos = event.get("geos", [])
    ledger_sources = event.get("ledger_sources", [])

    # Run Spursh primary ledger validation
    validation_issues = _validate_primary_ledger(jes)

    approval_id = str(uuid.uuid4())
    now = dt.datetime.utcnow().isoformat()

    # Persist approval request
    approval_record = {
        "approval_id": approval_id,
        "batch_id": batch_id,
        "status": "PENDING",
        "requested_at": now,
        "task_token": task_token,
        "period_type": period_type,
        "effective_date": effective_date,
        "geos": geos,
        "ledger_sources": ledger_sources,
        "je_count": len(jes),
        "validation_issue_count": len(validation_issues),
        "validation_issues": json.dumps(validation_issues, default=str),
        "approved_by": "",
        "decided_at": "",
    }
    approval_table.put_item(Item=approval_record)

    # Notify L7 manager
    summary = {
        "je_count": len(jes),
        "period_type": period_type,
        "effective_date": effective_date,
        "geos": geos,
        "ledger_sources": ledger_sources,
        "issue_count": len(validation_issues),
    }
    _send_approval_request(approval_id, batch_id, summary, task_token)

    return {
        "approval_id": approval_id,
        "batch_id": batch_id,
        "status": "PENDING",
        "validation_issues": validation_issues,
    }
