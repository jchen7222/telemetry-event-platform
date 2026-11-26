import json

import boto3
import pytest
from moto import mock_aws

from eventlog import from_warehouse as fw
from eventlog.envelope import validate
from eventlog.ingest import handler
from eventlog.projections import apply_from_log, connect
from tests.test_ingest import kinesis_event

ROWS = [
    {"reading_id": "a1", "facility_id": "F1", "industry": "chem", "source_name": "chem_scada",
     "reading_ts": "2024-03-01T00:00:00", "energy_kwh": 1.25, "pipeline_run_id": "r1",
     "loaded_at": "2024-03-02T00:00:00"},
    {"reading_id": "b2", "facility_id": "F2", "industry": "steel", "source_name": "steel_meters",
     "reading_ts": "2024-03-01T01:00:00", "energy_kwh": 2.5, "pipeline_run_id": "r1",
     "loaded_at": "2024-03-02T00:00:00"},
]


def _write(tmp_path, rows):
    p = tmp_path / "energy_readings_v1.jsonl"
    p.write_text("".join(json.dumps(r) + "\n" for r in rows))
    return str(p)


def test_read_contract_and_total(tmp_path):
    rows = fw.read_contract(_write(tmp_path, ROWS))
    assert len(rows) == 2 and fw.total_kwh(rows) == 3.75


def test_read_contract_rejects_missing_column(tmp_path):
    bad = [dict(ROWS[0], energy_kwh=None)]
    with pytest.raises(fw.ContractError):
        fw.read_contract(_write(tmp_path, bad))


def test_events_are_valid_envelopes():
    for ev in fw.to_events(ROWS):
        assert validate(dict(ev))["unit"] == "kWh"


def test_reconcile_tolerance():
    assert fw.reconcile(3.75, 3.7500004)["reconciled"] is True
    assert fw.reconcile(3.75, 2.5)["reconciled"] is False


@mock_aws
def test_contract_rows_through_handler_and_projections(tmp_path):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="log")
    rows = fw.read_contract(_write(tmp_path, ROWS))
    counts = handler(kinesis_event(*fw.to_events(rows)), s3_client=s3, bucket="log", run_id="r1")
    assert counts["landed"] == 2 and counts["reconciled"] is True

    db = str(tmp_path / "p.duckdb")
    apply_from_log(s3, "log", "r1", database=db)
    con = connect(db)
    projected = con.execute("select sum(energy_kwh) from hourly_rollup").fetchone()[0]
    con.close()
    assert fw.reconcile(fw.total_kwh(rows), projected)["reconciled"] is True
