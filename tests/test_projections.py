import json

import boto3
from moto import mock_aws

from eventlog.projections import apply_from_log, as_of, connect, record_hop

BUCKET = "test-log"


def _stored(event_id, facility, reading_ts, value, ingested_at, flag="OK"):
    return {"schema_version": 2, "event_id": event_id, "source_id": "chem_gw",
            "facility_id": facility, "reading_ts": reading_ts, "value": value,
            "unit": "kWh", "quality_flag": flag, "sent_at": reading_ts,
            "pipeline_run_id": "r1", "ingested_at": ingested_at}


def _put(s3, ev):
    key = f"events/chem_gw/{ev['reading_ts'][:10]}/{ev['event_id']}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(ev).encode())


@mock_aws
def test_apply_is_idempotent(tmp_path):
    db = str(tmp_path / "p.duckdb")
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    _put(s3, _stored("a1", "CHM-1", "2024-03-04 09:00:00", 900.0, "2024-03-05 10:00:00"))
    _put(s3, _stored("a2", "CHM-1", "2024-03-04 10:00:00", 950.0, "2024-03-05 10:00:00"))

    first = apply_from_log(s3, BUCKET, "r1", database=db)
    again = apply_from_log(s3, BUCKET, "r1", database=db)
    assert first["applied"] == 2 and again["applied"] == 0 and again["skipped"] == 2

    con = connect(db)
    assert con.execute("select count(*) from events_applied").fetchone()[0] == 2
    con.close()


@mock_aws
def test_correction_changes_now_but_not_the_past(tmp_path):
    db = str(tmp_path / "p.duckdb")
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    ts = "2024-03-04 09:00:00"
    _put(s3, _stored("orig", "CHM-1", ts, 900.0, "2024-03-05 10:00:00"))
    _put(s3, _stored("rev", "CHM-1", ts, 875.5, "2024-03-05 12:00:00", flag="REVISED"))
    apply_from_log(s3, BUCKET, "r1", database=db)

    con = connect(db)
    now_value = con.execute(
        "select value_kwh from current_state where facility_id = 'CHM-1'").fetchone()[0]
    con.close()
    assert now_value == 875.5                       # current belief: the revision

    belief_at_11 = as_of("2024-03-05 11:00:00", database=db)
    assert belief_at_11 == [("CHM-1",) + r[1:] for r in belief_at_11]
    assert belief_at_11[0][2] == 900.0              # the past is reproducible

    belief_at_13 = as_of("2024-03-05 13:00:00", database=db)
    assert belief_at_13[0][2] == 875.5


def test_record_hop_flags_mismatch(tmp_path):
    db = str(tmp_path / "p.duckdb")
    assert record_hop("r1", "a->b", 10, 10, database=db) is True
    assert record_hop("r1", "b->c", 10, 9, database=db) is False
    con = connect(db)
    statuses = dict(con.execute("select hop, status from run_audit").fetchall())
    con.close()
    assert statuses == {"a->b": "RECONCILED", "b->c": "MISMATCH"}
