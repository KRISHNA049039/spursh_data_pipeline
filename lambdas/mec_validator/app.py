"""
MEC Validation Gate — Lambda
Enforces three hard checks before any batch can proceed to OFA:
  1. 24-column completeness (all columns present, non-nullable fields populated)
  2. Dr = Cr balance per journal_id (zero tolerance)
  3. 7-segment COA validity against reference data
If ANY check fails the batch is rejected and quarantined.
"""
import json
import os
from collections import defaultdict
from decimal import Decimal, InvalidOperation

import boto3

s3 = boto3.client("s3")
quarantine_bucket = os.environ["QUARANTINE_BUCKET"]

# ---------------------------------------------------------------------------
# Canonical 24-column contract
# ---------------------------------------------------------------------------
REQUIRED_24 = [
    "surrogate_key", "source_system", "journal_id", "journal_line_num",
    "ledger_id", "effective_date", "period_name", "currency_code",
    "entered_dr", "entered_cr", "accounted_dr", "accounted_cr", "fx_rate",
    "segment1_entity", "segment2_cost_center", "segment3_account",
    "segment4_project", "segment5_intercompany", "segment6_product",
    "segment7_spare", "description", "batch_id", "load_timestamp", "payload_hash",
]

# Amount columns are the only ones allowed to be null
NULLABLE_COLUMNS = {"entered_dr", "entered_cr", "accounted_dr", "accounted_cr"}

# The 7 COA segment columns
SEGMENT_COLUMNS = [
    "segment1_entity",
    "segment2_cost_center",
    "segment3_account",
    "segment4_project",
    "segment5_intercompany",
    "segment6_product",
    "segment7_spare",
]


# ---------------------------------------------------------------------------
# Reference data loader (segment validation)
# ---------------------------------------------------------------------------
# In production these come from a DynamoDB table or S3 reference file.
# For now we accept a reference dict passed in the event, or fall back to
# a permissive default that only checks for non-empty values.

def _load_reference_data(event):
    """Return a dict mapping each segment column to a set of valid values."""
    ref = event.get("reference_data", {})
    return {
        "segment1_entity":      set(ref.get("entities", [])),
        "segment2_cost_center": set(ref.get("cost_centers", [])),
        "segment3_account":     set(ref.get("accounts", [])),
        "segment4_project":     set(ref.get("projects", [])),
        "segment5_intercompany": set(ref.get("ic_entities", [])),
        "segment6_product":     set(ref.get("products", [])),
        "segment7_spare":       set(ref.get("spare_values", [])),
    }


def _to_decimal(value):
    """Safely convert a value to Decimal; treat None/blank as zero."""
    if value is None or value == "" or value == "null":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


# ---------------------------------------------------------------------------
# CHECK 1 — 24-column completeness
# ---------------------------------------------------------------------------
def check_24_columns(rows):
    """Verify all 24 columns exist and non-nullable fields are populated."""
    issues = []

    if not rows:
        return {
            "check": "24_column_completeness",
            "passed": False,
            "issues": [{"row": None, "type": "no_journal_lines",
                        "details": "No journal lines supplied."}],
        }

    # Check column presence on first row (all rows share the same keys)
    sample_keys = set(rows[0].keys()) if rows else set()
    missing_columns = [c for c in REQUIRED_24 if c not in sample_keys]
    if missing_columns:
        issues.append({
            "row": None,
            "type": "missing_columns",
            "details": missing_columns,
        })

    # Check non-nullable fields per row
    non_nullable = [c for c in REQUIRED_24 if c not in NULLABLE_COLUMNS]
    for idx, row in enumerate(rows, start=1):
        nulls = [c for c in non_nullable
                 if c in row and (row[c] is None or str(row[c]).strip() == "")]
        if nulls:
            issues.append({
                "row": idx,
                "type": "null_violation",
                "details": nulls,
            })

    return {
        "check": "24_column_completeness",
        "passed": len(issues) == 0,
        "issues": issues,
    }


# ---------------------------------------------------------------------------
# CHECK 2 — Debit equals Credit per journal_id (zero tolerance)
# ---------------------------------------------------------------------------
def check_dr_equals_cr(rows):
    """Sum(accounted_dr) must equal Sum(accounted_cr) per journal_id."""
    issues = []
    journals = defaultdict(lambda: {"dr": Decimal("0"), "cr": Decimal("0")})

    for row in rows:
        jid = row.get("journal_id", "UNKNOWN")
        journals[jid]["dr"] += _to_decimal(row.get("accounted_dr"))
        journals[jid]["cr"] += _to_decimal(row.get("accounted_cr"))

    imbalanced = {
        jid: {"accounted_dr": str(totals["dr"]), "accounted_cr": str(totals["cr"])}
        for jid, totals in journals.items()
        if totals["dr"] != totals["cr"]
    }

    if imbalanced:
        issues.append({
            "row": None,
            "type": "dr_cr_imbalance",
            "details": imbalanced,
        })

    return {
        "check": "dr_cr_balance",
        "passed": len(issues) == 0,
        "issues": issues,
    }


# ---------------------------------------------------------------------------
# CHECK 3 — 7-segment COA validity
# ---------------------------------------------------------------------------
def check_7_segments(rows, reference_data):
    """Each segment value must exist in its reference set (if provided)."""
    issues = []
    violations = defaultdict(set)

    for idx, row in enumerate(rows, start=1):
        for seg_col in SEGMENT_COLUMNS:
            value = row.get(seg_col)

            # Segment must not be empty/null
            if value is None or str(value).strip() == "":
                violations[seg_col].add(f"<empty> (row {idx})")
                continue

            # If reference data is provided for this segment, validate membership
            valid_set = reference_data.get(seg_col, set())
            if valid_set and value not in valid_set:
                violations[seg_col].add(f"{value} (row {idx})")

    if violations:
        issues.append({
            "row": None,
            "type": "segment_violations",
            "details": {k: sorted(v) for k, v in violations.items()},
        })

    return {
        "check": "7_segment_validity",
        "passed": len(issues) == 0,
        "issues": issues,
    }


# ---------------------------------------------------------------------------
# Validation orchestrator — fail-fast
# ---------------------------------------------------------------------------
def run_mec_validation(rows, reference_data):
    """Run all three MEC checks. If any fails, the batch is rejected."""
    results = [
        check_24_columns(rows),
        check_dr_equals_cr(rows),
        check_7_segments(rows, reference_data),
    ]

    failed = [r for r in results if not r["passed"]]
    all_issues = []
    for r in results:
        all_issues.extend(r["issues"])

    return {
        "is_valid": len(failed) == 0,
        "checks_run": len(results),
        "checks_passed": len(results) - len(failed),
        "checks_failed": len(failed),
        "results": results,
        "issues": all_issues,
    }


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------
def handler(event, _context):
    batch_id = event.get("batch_id", "UNKNOWN")
    journal_lines = event.get("journal_lines", [])
    reference_data = _load_reference_data(event)

    validation = run_mec_validation(journal_lines, reference_data)
    validation["batch_id"] = batch_id

    # Quarantine failed batches
    if not validation["is_valid"]:
        key = f"mec_failures/{batch_id}.json"
        s3.put_object(
            Bucket=quarantine_bucket,
            Key=key,
            Body=json.dumps(
                {
                    "batch_id": batch_id,
                    "source_bucket": event.get("source_bucket"),
                    "source_key": event.get("source_key"),
                    "validation": validation,
                },
                default=str,
            ).encode("utf-8"),
            ContentType="application/json",
        )
        validation["quarantine_key"] = key

    return validation
