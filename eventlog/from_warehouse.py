"""Read the warehouse's versioned public contract as a telemetry source.

template-driven-ingestion exports energy_readings_v1.jsonl: one row per
reading, already normalized to kWh. This module turns those rows into v2
envelopes so they go through the same validate -> ingest -> projection path
as any gateway, and owns the tolerance for the cross-repository kWh check.
"""
from __future__ import annotations

import json
import math

CONTRACT_COLUMNS = ("reading_id", "facility_id", "industry", "source_name",
                    "reading_ts", "energy_kwh", "pipeline_run_id", "loaded_at")

# Absolute kWh tolerance. The exporter prints total_kwh to 6 decimal places,
# so anything beyond rounding noise means readings were lost or duplicated.
KWH_TOLERANCE = 1e-3


class ContractError(ValueError):
    """A row in the export does not match the contract the exporter promised."""


def read_contract(path: str) -> list[dict]:
    """Read the JSONL export. Every row must carry every contracted column."""
    rows = []
    with open(path, encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            if not line.strip():
                continue
            row = json.loads(line)
            missing = [c for c in CONTRACT_COLUMNS if row.get(c) in (None, "")]
            if missing:
                raise ContractError(f"line {lineno}: missing contracted columns: "
                                    f"{', '.join(missing)}")
            rows.append(row)
    return rows


def to_events(rows: list[dict]) -> list[dict]:
    """Map contract rows to canonical v2 envelopes (kWh, quality OK)."""
    return [{
        "schema_version": 2,
        "event_id": row["reading_id"],
        "source_id": row["source_name"],
        "facility_id": row["facility_id"],
        "reading_ts": row["reading_ts"],
        "value": float(row["energy_kwh"]),
        "unit": "kWh",
        "quality_flag": "OK",
        "sent_at": row["loaded_at"],
    } for row in rows]


def total_kwh(rows: list[dict]) -> float:
    return math.fsum(float(r["energy_kwh"]) for r in rows)


def reconcile(expected_kwh: float, actual_kwh: float,
              tolerance: float = KWH_TOLERANCE) -> dict:
    diff = abs(float(expected_kwh) - float(actual_kwh))
    return {"expected_kwh": float(expected_kwh), "actual_kwh": float(actual_kwh),
            "diff_kwh": diff, "tolerance_kwh": tolerance, "reconciled": diff <= tolerance}
