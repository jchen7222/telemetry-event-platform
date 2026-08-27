"""Deterministic synthetic gateway traffic for the demo pipeline.

Three sources with different envelope versions and units, plus the failure
modes a real fleet produces: malformed JSON, negative values, unknown schema
versions, missing fields, device clock faults, an exact duplicate delivery,
and a late correction wave (wave2_*) that revises three readings after the
fact. All synthetic, fixed seed.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import random
from datetime import datetime, timedelta

random.seed(11)
START = datetime(2024, 3, 4)
HOURS = 48
OUT = "data/devices"


def eid(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


def shape(ts: datetime) -> float:
    return 0.6 + 0.4 * math.sin((ts.hour - 6) / 24 * 2 * math.pi) ** 2


def write(name: str, lines: list[str]) -> None:
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, name)
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote {path} ({len(lines)} events)")


def steel() -> None:
    """Legacy gateways still on envelope v1 (implicit kWh)."""
    lines = []
    for h in range(HOURS):
        ts = START + timedelta(hours=h)
        for fac in ["STL-001", "STL-002", "STL-003", "STL-004"]:
            lines.append(json.dumps({
                "schema_version": 1,
                "event_id": eid("steel", fac, ts.isoformat()),
                "source_id": "steel_gw",
                "facility_id": fac,
                "reading_ts": f"{ts:%Y-%m-%d %H:%M:%S}",
                "kwh": round(1200 * shape(ts) * random.uniform(0.9, 1.1), 1),
                "sent_at": f"{ts + timedelta(minutes=2):%Y-%m-%d %H:%M:%S}",
            }))
    write("steel_gw.jsonl", lines)


def textile() -> None:
    """Current envelope v2, unit MWh."""
    lines = []
    for h in range(HOURS):
        ts = START + timedelta(hours=h)
        for fac in ["TX-A", "TX-B", "TX-C"]:
            lines.append(json.dumps({
                "schema_version": 2,
                "event_id": eid("textile", fac, ts.isoformat()),
                "source_id": "textile_gw",
                "facility_id": fac,
                "reading_ts": f"{ts:%Y-%m-%d %H:%M:%S}",
                "value": round(0.65 * shape(ts) * random.uniform(0.85, 1.15), 4),
                "unit": "MWh",
                "quality_flag": "OK",
                "sent_at": f"{ts + timedelta(minutes=1):%Y-%m-%d %H:%M:%S}",
            }))
    write("textile_gw.jsonl", lines)


def chem() -> None:
    """Envelope v2, unit MJ — the dirty feed, including one duplicate delivery."""
    lines = []
    for h in range(HOURS):
        ts = START + timedelta(hours=h)
        for fac in ["CHM-1", "CHM-2", "CHM-3"]:
            lines.append(json.dumps({
                "schema_version": 2,
                "event_id": eid("chem", fac, ts.isoformat()),
                "source_id": "chem_gw",
                "facility_id": fac,
                "reading_ts": f"{ts:%Y-%m-%d %H:%M:%S}",
                "value": round(3600 * shape(ts) * random.uniform(0.8, 1.2), 1),
                "unit": "MJ",
                "quality_flag": "OK",
                "sent_at": f"{ts + timedelta(minutes=3):%Y-%m-%d %H:%M:%S}",
            }))
    # failure modes, injected at fixed positions
    good = json.loads(lines[10])
    lines.insert(30, lines[10])                                # exact duplicate delivery
    bad_neg = dict(good, event_id=eid("chem", "neg"), value=-52.0)
    lines.insert(60, json.dumps(bad_neg))                      # negative energy
    bad_ver = dict(good, event_id=eid("chem", "ver"), schema_version=9)
    lines.insert(90, json.dumps(bad_ver))                      # unknown schema version
    bad_missing = {k: v for k, v in good.items() if k != "facility_id"}
    bad_missing["event_id"] = eid("chem", "mis")
    lines.insert(120, json.dumps(bad_missing))                 # missing required field
    bad_ts = dict(good, event_id=eid("chem", "ts"), reading_ts="not-a-time")
    lines.insert(150, json.dumps(bad_ts))                      # device clock fault
    bad_unit = dict(good, event_id=eid("chem", "unit"), unit="BTU")
    lines.insert(180, json.dumps(bad_unit))                    # config drift
    lines.insert(210, '{"schema_version": 2, "event_id": "trunc')  # truncated packet
    write("chem_gw.jsonl", lines)


def corrections() -> None:
    """Wave 2: QA revises three chem readings after the fact.

    Same (facility, reading_ts), new event_id, later ingestion — the log keeps
    both beliefs, current_state shows the revision, as_of(T) shows the original.
    """
    lines = []
    for fac, hour, revised in [("CHM-1", 9, 2410.0), ("CHM-2", 9, 2735.5), ("CHM-3", 10, 3105.2)]:
        ts = START + timedelta(hours=hour)
        lines.append(json.dumps({
            "schema_version": 2,
            "event_id": eid("chem", fac, ts.isoformat(), "rev2"),
            "source_id": "chem_gw",
            "facility_id": fac,
            "reading_ts": f"{ts:%Y-%m-%d %H:%M:%S}",
            "value": revised,
            "unit": "MJ",
            "quality_flag": "REVISED",
            "sent_at": f"{START + timedelta(hours=30):%Y-%m-%d %H:%M:%S}",
        }))
    write("wave2_corrections.jsonl", lines)


if __name__ == "__main__":
    steel()
    textile()
    chem()
    corrections()
