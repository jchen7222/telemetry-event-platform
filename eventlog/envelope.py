"""Event envelope: validation, schema versions, and the v1 → v2 upgrade path.

Gateways in the field do not upgrade on our schedule, so two envelope versions
are live at once. Version is explicit on every event; the platform upgrades old
versions to the canonical form at the edge, and rejects versions it does not
know — into quarantine with a reason, never by crashing the batch.

  v1 (legacy gateways):  {schema_version: 1, event_id, source_id, facility_id,
                          reading_ts, kwh, sent_at}
  v2 (current):          {schema_version: 2, event_id, source_id, facility_id,
                          reading_ts, value, unit, quality_flag, sent_at}
"""
from __future__ import annotations

from datetime import datetime

KNOWN_VERSIONS = {1, 2}
UNIT_TO_KWH = {"kWh": 1.0, "MWh": 1000.0, "MJ": 0.2777778}

V1_REQUIRED = {"event_id", "source_id", "facility_id", "reading_ts", "kwh", "sent_at"}
V2_REQUIRED = {"event_id", "source_id", "facility_id", "reading_ts", "value", "unit",
               "quality_flag", "sent_at"}


class EnvelopeError(Exception):
    """Raised with a human-readable reason; the reason becomes the quarantine record."""


def _parse_ts(value) -> datetime:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, TypeError):
        raise EnvelopeError("unparseable timestamp")


def upgrade_to_v2(event: dict) -> dict:
    """v1 events measured kWh implicitly; make that explicit and mark quality unknown-good."""
    return {
        "schema_version": 2,
        "event_id": event["event_id"],
        "source_id": event["source_id"],
        "facility_id": event["facility_id"],
        "reading_ts": event["reading_ts"],
        "value": event["kwh"],
        "unit": "kWh",
        "quality_flag": "OK",
        "sent_at": event["sent_at"],
    }


def validate(event: dict) -> dict:
    """Return the event in canonical (v2) form, or raise EnvelopeError with a reason."""
    if not isinstance(event, dict):
        raise EnvelopeError("payload is not an object")
    version = event.get("schema_version")
    if version not in KNOWN_VERSIONS:
        raise EnvelopeError(f"unknown schema_version: {version!r}")

    required = V1_REQUIRED if version == 1 else V2_REQUIRED
    missing = [f for f in sorted(required) if event.get(f) in (None, "")]
    if missing:
        raise EnvelopeError(f"missing required fields: {', '.join(missing)}")

    if version == 1:
        event = upgrade_to_v2(event)

    _parse_ts(event["reading_ts"])
    _parse_ts(event["sent_at"])
    if event["unit"] not in UNIT_TO_KWH:
        raise EnvelopeError(f"unknown unit: {event['unit']!r}")
    try:
        value = float(event["value"])
    except (ValueError, TypeError):
        raise EnvelopeError("non-numeric value")
    if value < 0:
        raise EnvelopeError("negative energy value")
    event["value"] = value
    return event


def value_kwh(event: dict) -> float:
    """Canonical events normalize to kWh at projection time, one rule for every unit."""
    return float(event["value"]) * UNIT_TO_KWH[event["unit"]]
