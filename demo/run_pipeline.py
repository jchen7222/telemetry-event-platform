"""End-to-end demo: gateways → Kinesis → Lambda handler → S3 log → projections.

Runs the real handler code against moto-mocked AWS, entirely in-process — no
credentials, no cost, same code paths as a deployment (infra/template.yaml).

Two waves, to demonstrate the audit story:
  wave 1: normal gateway traffic (including chem_gw's dirty rows)
  wave 2: late corrections — after wave 1 is projected, we note the time T,
          then ingest revisions. current_state shows the revised belief;
          as_of(T) still reproduces exactly what we believed before.
"""
from __future__ import annotations

import base64
import glob
import json
import os
import sys
import time
import uuid

import boto3
from moto import mock_aws

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from eventlog.ingest import handler  # noqa: E402
from eventlog.projections import apply_from_log, as_of, connect, record_hop  # noqa: E402

BUCKET = "telemetry-event-log"
STREAM = "telemetry-readings"
REGION = "us-east-1"
SNAPSHOT = "ops_snapshot.json"


def produce(kinesis, files: list[str]) -> int:
    produced = 0
    for path in files:
        lines = [ln for ln in open(path).read().splitlines() if ln.strip()]
        for i in range(0, len(lines), 100):
            batch = [{"Data": ln.encode(), "PartitionKey": str(hash(ln) % 4)}
                     for ln in lines[i:i + 100]]
            kinesis.put_records(StreamName=STREAM, Records=batch)
            produced += len(batch)
    return produced


def drain(kinesis) -> list[dict]:
    """Fetch everything currently in the stream, shaped exactly like Lambda receives it."""
    batches = []
    shards = kinesis.list_shards(StreamName=STREAM)["Shards"]
    for shard in shards:
        it = kinesis.get_shard_iterator(
            StreamName=STREAM, ShardId=shard["ShardId"],
            ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        while it:
            out = kinesis.get_records(ShardIterator=it, Limit=100)
            if not out["Records"]:
                break
            batches.append({"Records": [
                {"kinesis": {"data": base64.b64encode(r["Data"]).decode(),
                             "partitionKey": r["PartitionKey"],
                             "sequenceNumber": r["SequenceNumber"]}}
                for r in out["Records"]]})
            it = out.get("NextShardIterator")
    return batches


def run_wave(kinesis, s3, files: list[str], run_id: str) -> dict:
    produced = produce(kinesis, files)
    totals = {"records": 0, "landed": 0, "duplicates": 0, "quarantined": 0}
    for lambda_event in drain(kinesis):
        counts = handler(lambda_event, s3_client=s3, bucket=BUCKET, run_id=run_id)
        for k in totals:
            totals[k] += counts[k]
    totals["produced"] = produced
    return totals


def main() -> int:
    db = os.environ.get("PROJECTIONS_DB", "projections.duckdb")
    if os.path.exists(db):
        os.remove(db)
    run_id = uuid.uuid4().hex[:12]
    files = sorted(glob.glob("data/devices/*.jsonl"))
    wave1 = [f for f in files if "wave2" not in f]
    wave2 = [f for f in files if "wave2" in f]
    ok = True

    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        kinesis = boto3.client("kinesis", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        s3.put_bucket_versioning(Bucket=BUCKET,
                                 VersioningConfiguration={"Status": "Enabled"})
        kinesis.create_stream(StreamName=STREAM, ShardCount=2)

        # ---- wave 1: normal traffic --------------------------------------
        t1 = run_wave(kinesis, s3, wave1, run_id)
        ok &= record_hop(run_id, "gateway->stream", t1["produced"], t1["records"],
                         "records fetched from Kinesis vs produced")
        ok &= record_hop(run_id, "stream->log",
                         t1["records"],
                         t1["landed"] + t1["duplicates"] + t1["quarantined"],
                         "landed + duplicates collapsed + quarantined")
        p1 = apply_from_log(s3, BUCKET, run_id)
        ok &= record_hop(run_id, "log->projections", p1["log_objects"],
                         p1["applied"] + p1["skipped"], "events applied (idempotent)")
        print(f"wave 1: produced={t1['produced']} landed={t1['landed']} "
              f"duplicates={t1['duplicates']} quarantined={t1['quarantined']} "
              f"projected={p1['applied']}")

        # note the moment between waves — the as_of demo pivots on it
        time.sleep(1.1)
        t_mid = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        time.sleep(1.1)

        # ---- wave 2: late corrections ------------------------------------
        t2 = run_wave(kinesis, s3, wave2, run_id)
        p2 = apply_from_log(s3, BUCKET, run_id)
        ok &= record_hop(run_id, "corrections->log", t2["produced"], t2["landed"],
                         "revision events landed")
        print(f"wave 2: corrections landed={t2['landed']} projected={p2['applied']} "
              f"(previously applied skipped={p2['skipped']})")

        # ---- audit story -------------------------------------------------
        con = connect()
        revised = con.execute(
            """select facility_id, reading_ts, round(value_kwh, 1)
               from current_state where quality_flag = 'REVISED'
               order by facility_id""").fetchall()
        con.close()
        before = {(r[0], str(r[1])): r[2] for r in as_of(t_mid)}
        print("\nthe audit question — same reading, two beliefs:")
        for fac, ts, now_kwh in revised:
            old = before.get((fac, str(ts)))
            old = round(old, 1) if old is not None else "?"
            print(f"  {fac} {ts}: believed {old} kWh at {t_mid}, "
                  f"believe {round(now_kwh, 1)} kWh now — both reproducible")

        # ---- snapshot for the ops dashboard ------------------------------
        quarantine = []
        pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET,
                                                             Prefix="quarantine/")
        for page in pages:
            for obj in page.get("Contents", []):
                quarantine.append(json.loads(
                    s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()))
        with open(SNAPSHOT, "w") as f:
            json.dump({"run_id": run_id, "wave1": t1, "wave2": t2,
                       "as_of_pivot": t_mid, "quarantine": quarantine}, f, indent=2)

    print(f"\nhops {'RECONCILED' if ok else 'MISMATCH'}; snapshot -> {SNAPSHOT}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
