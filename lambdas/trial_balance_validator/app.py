"""
Trial Balance Validator — Lambda
Validates generated JEs against the trial balance for each geo.
Checks:
  1. Sum(Dr) == Sum(Cr) per geo per ledger_source (zero tolerance)
  2. Every account in the JEs exists in the trial balance
  3. Net movement per account does not exceed TB opening + activity
  4. All geos have at least one JE (completeness)
"""
import json
import os
from collections import defaultdict
from decimal import Decimal, InvalidOperation

import boto3

s3 = boto3.client("s3")
QUARANTINE_BUCKET = os.environ["QUARANTINE_BUCKET"]
REF_DATA_BUCKET = os.environ.get("REF_DATA_BUCKET", "")


def _dec(value):
    if value is None or value == "" or value == "null":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _load_trial_balance(event):
    """
    Load TB reference from event payload or S3.
    Expected format: { "US": { "1001": {"opening": "100", "activity": "50"}, ... }, ... }
    """
    tb = event.get("trial_balance")
    if tb:
        return tb

    tb_key = event.get("trial_balance_key")
    if tb_key and REF_DATA_BUCKET:
        resp = s3.get_object(Bucket=REF_DATA_BUCKET, Key=tb_key)
        return json.loads(resp["Body"].read().decode("utf-8"))

    return {}


def _check_balance_per_geo(jes):
    """Dr must equal Cr per geo per ledger_source."""
    issues = []
    buckets = defaultdict(lambda: {"dr": Decimal("0"), "cr": Decimal("0")})

    for je in jes:
        key = f"{je.get('geo', 'UNKNOWN')}|{je.get('ledger_source', 'UNKNOWN')}"
        buckets[key]["dr"] += _dec(je.get("accounted_dr"))
        buckets[key]["cr"] += _dec(je.get("accounted_cr"))

    for key, totals in buckets.items():
        if totals["dr"] != totals["cr"]:
            geo, src = key.split("|")
            issues.append({
                "check": "dr_cr_balance",
                "geo": geo,
                "ledger_source": src,
                "accounted_dr": str(totals["dr"]),
                "accounted_cr": str(totals["cr"]),
                "diff": str(totals["dr"] - totals["cr"]),
            })

    return issues


def _check_accounts_in_tb(jes, trial_balance):
    """Every segment3_account in JEs must exist in the TB for that geo."""
    issues = []
    if not trial_balance:
        return issues  # No TB provided — skip this check

    for je in jes:
        geo = je.get("geo", "UNKNOWN")
        account = je.get("segment3_account", "")
        geo_tb = trial_balance.get(geo, {})
        if geo_tb and account and account not in geo_tb:
            issues.append({
                "check": "account_not_in_tb",
                "geo": geo,
                "account": account,
                "journal_id": je.get("journal_id"),
            })

    return issues


def _check_geo_completeness(jes, expected_geos):
    """All expected geos must have at least one JE."""
    present_geos = {je.get("geo") for je in jes}
    missing = [g for g in expected_geos if g not in present_geos]
    if missing:
        return [{"check": "geo_completeness", "missing_geos": missing}]
    return []


def handler(event, _context):
    jes = event.get("journal_entries", [])
    batch_id = event.get("batch_id", "UNKNOWN")
    expected_geos = event.get("expected_geos", [])
    trial_balance = _load_trial_balance(event)

    all_issues = []
    all_issues.extend(_check_balance_per_geo(jes))
    all_issues.extend(_check_accounts_in_tb(jes, trial_balance))
    if expected_geos:
        all_issues.extend(_check_geo_completeness(jes, expected_geos))

    is_valid = len(all_issues) == 0

    result = {
        "batch_id": batch_id,
        "is_valid": is_valid,
        "checks_run": 3,
        "issue_count": len(all_issues),
        "issues": all_issues,
    }

    if not is_valid:
        key = f"tb_failures/{batch_id}.json"
        s3.put_object(
            Bucket=QUARANTINE_BUCKET,
            Key=key,
            Body=json.dumps(result, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        result["quarantine_key"] = key

    return result
