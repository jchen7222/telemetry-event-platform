import base64
import json

import boto3
from moto import mock_aws

from eventlog.ingest import handler
from tests.test_envelope import V1, V2

BUCKET = "test-log"


def kinesis_event(*payloads) -> dict:
    return {"Records": [
        {"kinesis": {"data": base64.b64encode(
            (p if isinstance(p, str) else json.dumps(p)).encode()).decode()}}
        for p in payloads]}


def _setup():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    return s3


def _count(s3, prefix):
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return resp.get("KeyCount", 0)


@mock_aws
def test_valid_events_land_and_reconcile():
    s3 = _setup()
    counts = handler(kinesis_event(V1, V2), s3_client=s3, bucket=BUCKET, run_id="r1")
    assert counts == {"records": 2, "landed": 2, "duplicates": 0,
                      "quarantined": 0, "reconciled": True}
    assert _count(s3, "events/") == 2


@mock_aws
def test_at_least_once_delivery_collapses():
    s3 = _setup()
    handler(kinesis_event(V1, V2), s3_client=s3, bucket=BUCKET, run_id="r1")
    counts = handler(kinesis_event(V1, V2), s3_client=s3, bucket=BUCKET, run_id="r1")
    assert counts["duplicates"] == 2 and counts["landed"] == 0
    assert _count(s3, "events/") == 2          # nothing accumulated


@mock_aws
def test_bad_events_quarantine_not_crash():
    s3 = _setup()
    bad_version = dict(V2, event_id="x1", schema_version=9)
    counts = handler(kinesis_event(V1, "not json{{", bad_version),
                     s3_client=s3, bucket=BUCKET, run_id="r1")
    assert counts["landed"] == 1 and counts["quarantined"] == 2
    assert counts["reconciled"] is True
    body = s3.get_object(
        Bucket=BUCKET,
        Key=s3.list_objects_v2(Bucket=BUCKET, Prefix="quarantine/")["Contents"][0]["Key"],
    )["Body"].read()
    assert b"reason" in body and b"payload" in body
