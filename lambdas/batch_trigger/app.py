import json
import os
import uuid
from urllib.parse import unquote_plus

import boto3


s3 = boto3.client("s3")
stepfunctions = boto3.client("stepfunctions")
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]


def _load_journal_lines(bucket, key):
    if not key.lower().endswith(".json"):
        return []

    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    payload = json.loads(body)

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if isinstance(payload.get("journal_lines"), list):
            return payload["journal_lines"]
        if isinstance(payload.get("records"), list):
            return payload["records"]

    return []


def handler(event, _context):
    records = event.get("Records", [])
    started = []

    for record in records:
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        batch_id = f"arfx-{uuid.uuid4()}"
        journal_lines = _load_journal_lines(bucket, key)

        execution_input = {
            "batch_id": batch_id,
            "source_system": "AR_FX",
            "source_bucket": bucket,
            "source_key": key,
            "journal_lines": journal_lines,
        }

        response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=batch_id,
            input=json.dumps(execution_input),
        )

        started.append(
            {
                "batch_id": batch_id,
                "execution_arn": response["executionArn"],
                "source_bucket": bucket,
                "source_key": key,
            }
        )

    return {"started": started}
