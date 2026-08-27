import json

from eventlog.dashboard import diagnose, render
from eventlog.projections import connect, record_hop
import yaml


def test_reasons_map_to_known_patterns():
    patterns = yaml.safe_load(open("ops/patterns.yml"))["patterns"]
    d = diagnose("negative energy value", patterns)
    assert d["pattern"].startswith("Meter rollover")
    d = diagnose("something never seen before", patterns)
    assert d["pattern"] == "Unrecognized failure"


def test_render_produces_selfservice_page(tmp_path):
    db = str(tmp_path / "p.duckdb")
    con = connect(db)
    con.execute(
        "insert into events_applied values ('e1','chem_gw','CHM-1',"
        "'2024-03-04 09:00:00',900.0,'OK','2024-03-04 09:03:00',"
        "'2024-03-05 10:00:00','r1','events/x.json')")
    con.close()
    record_hop("r1", "stream->log", 5, 5, database=db)

    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps({
        "run_id": "r1",
        "wave1": {"produced": 5, "records": 5, "landed": 4, "duplicates": 0,
                  "quarantined": 1},
        "wave2": {"produced": 0, "records": 0, "landed": 0, "duplicates": 0,
                  "quarantined": 0},
        "as_of_pivot": "2024-03-05 11:00:00",
        "quarantine": [{"reason": "unparseable timestamp", "payload": "x",
                        "pipeline_run_id": "r1", "ingested_at": "2024-03-05 10:00:00"}],
    }))
    out = tmp_path / "dash.html"
    render(snapshot_path=str(snap), out_path=str(out), database=db)
    page = out.read_text()
    assert "Device clock fault" in page          # reason diagnosed via the registry
    assert "RECONCILED" in page
    assert "CHM-1" in page
