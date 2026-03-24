"""
Spursh Approval Callback — Lambda (API Gateway)
Handles L7 manager approve/reject decisions.
Resumes the Step Functions execution with the decision.
"""
import datetime as dt
import json
import os

import boto3

sfn_client = boto3.client("stepfunctions")
dynamodb = boto3.resource("dynamodb")

APPROVAL_TABLE = os.environ["APPROVAL_TABLE"]
approval_table = dynamodb.Table(APPROVAL_TABLE)


def handler(event, _context):
    """
    Invoked via API Gateway GET /approve?approval_id=...&action=APPROVE|REJECT&token=...
    """
    params = event.get("queryStringParameters") or {}
    approval_id = params.get("approval_id", "")
    action = params.get("action", "").upper()
    task_token = params.get("token", "")
    approver = params.get("approver", "L7-manager")

    if not approval_id or not task_token:
        return _response(400, "Missing approval_id or token.")

    if action not in ("APPROVE", "REJECT"):
        return _response(400, f"Invalid action: {action}. Use APPROVE or REJECT.")

    # Update approval record
    now = dt.datetime.utcnow().isoformat()
    try:
        approval_table.update_item(
            Key={"approval_id": approval_id},
            UpdateExpression="SET #s = :s, approved_by = :by, decided_at = :at",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": "APPROVED" if action == "APPROVE" else "REJECTED",
                ":by": approver,
                ":at": now,
            },
            ConditionExpression="attribute_exists(approval_id)",
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return _response(404, f"Approval {approval_id} not found.")


    # Resume Step Functions
    try:
        if action == "APPROVE":
            sfn_client.send_task_success(
                taskToken=task_token,
                output=json.dumps({
                    "approval_id": approval_id,
                    "decision": "APPROVED",
                    "approved_by": approver,
                    "decided_at": now,
                }),
            )
        else:
            sfn_client.send_task_failure(
                taskToken=task_token,
                error="SpurshRejected",
                cause=f"Batch rejected by {approver} at {now}",
            )
    except sfn_client.exceptions.TaskTimedOut:
        return _response(410, "Approval window expired. Task timed out.")
    except sfn_client.exceptions.InvalidToken:
        return _response(400, "Invalid or expired task token.")

    return _response(200, f"Batch {action.lower()}d by {approver}.")


def _response(status_code, message):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"message": message}),
    }
