"""The ingest Lambda: Kinesis batch in, immutable S3 event log out.

This is a real AWS Lambda handler — `handler(event, context)` receives the
standard Kinesis event shape (base64 records). Locally and in CI it runs
against moto-mocked AWS; deployed, the same file runs unchanged (see
infra/template.yaml).

Design rules:
  * The log is append-only. Every valid event becomes one S3 object whose key
    is derived from the event itself: events/<source>/<date>/<event_id>.json.
    Kinesis delivers at-least-once; a redelivered event maps to the same key,
    so duplicates collapse instead of accumulating. Nothing ever rewrites a
    different object.
  * Bad events are data, not exceptions. Anything that fails validation lands
    in quarantine/ with the reason and the original payload. One malformed
    gateway must never poison a batch.
  * Every record is counted. The handler returns landed / duplicate /
    quarantined counts so the caller can reconcile the hop:
    records_in == landed + duplicates + quarantined.
"""
from __future__ import annotations

import base64
import json
import os
import uuid
from datetime import datetime, timezone

from .envelope import EnvelopeError, validate

EVENTS_PREFIX = "events"
QUARANTINE_PREFIX = "quarantine"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")


def event_key(event: dict) -> str:
    date = str(event["reading_ts"])[:10]
    return f"{EVENTS_PREFIX}/{event['source_id']}/{date}/{event['event_id']}.json"


def _object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def handler(event, context=None, *, s3_client=None, bucket: str | None = None,
            run_id: str | None = None) -> dict:
    """Process one Kinesis batch. Returns hop counts for reconciliation.

    Deployed, Lambda calls `handler(event, context)` and the defaults wire up
    from the environment; tests and the demo inject their own client/bucket.
    """
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")
    bucket = bucket or os.environ["EVENT_LOG_BUCKET"]
    run_id = run_id or (getattr(context, "aws_request_id", None) or uuid.uuid4().hex[:12])
    s3 = s3_client
    counts = {"records": 0, "landed": 0, "duplicates": 0, "quarantined": 0}
    ingested_at = _now_iso()

    for record in event.get("Records", []):
        counts["records"] += 1
        raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload, reason = None, "unparseable JSON"
        else:
            try:
                clean = validate(payload)
                reason = None
            except EnvelopeError as exc:
                reason = str(exc)

        if reason is not None:
            counts["quarantined"] += 1
            qkey = f"{QUARANTINE_PREFIX}/{run_id}/{counts['records']:06d}.json"
            s3.put_object(Bucket=bucket, Key=qkey, Body=json.dumps({
                "reason": reason, "payload": raw,
                "pipeline_run_id": run_id, "ingested_at": ingested_at,
            }).encode())
            continue

        key = event_key(clean)
        if _object_exists(s3, bucket, key):
            counts["duplicates"] += 1          # at-least-once delivery, collapsed
            continue
        stored = dict(clean)
        stored["pipeline_run_id"] = run_id
        stored["ingested_at"] = ingested_at
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(stored).encode())
        counts["landed"] += 1

    counts["reconciled"] = (
        counts["records"] == counts["landed"] + counts["duplicates"] + counts["quarantined"]
    )
    return counts
