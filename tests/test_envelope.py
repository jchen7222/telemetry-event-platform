import pytest

from eventlog.envelope import EnvelopeError, validate, value_kwh

V1 = {"schema_version": 1, "event_id": "e1", "source_id": "steel_gw",
      "facility_id": "STL-001", "reading_ts": "2024-03-04 10:00:00",
      "kwh": 1180.5, "sent_at": "2024-03-04 10:02:00"}
V2 = {"schema_version": 2, "event_id": "e2", "source_id": "textile_gw",
      "facility_id": "TX-A", "reading_ts": "2024-03-04 10:00:00",
      "value": 0.62, "unit": "MWh", "quality_flag": "OK",
      "sent_at": "2024-03-04 10:01:00"}


def test_v1_upgrades_to_canonical_v2():
    out = validate(dict(V1))
    assert out["schema_version"] == 2
    assert out["unit"] == "kWh"
    assert out["value"] == 1180.5
    assert out["quality_flag"] == "OK"


def test_v2_passes_and_units_normalize():
    out = validate(dict(V2))
    assert value_kwh(out) == pytest.approx(620.0)


@pytest.mark.parametrize("mutate,reason", [
    (lambda e: e.update(schema_version=9), "unknown schema_version"),
    (lambda e: e.pop("facility_id"), "missing required fields"),
    (lambda e: e.update(value=-4), "negative energy"),
    (lambda e: e.update(value="ERR"), "non-numeric"),
    (lambda e: e.update(unit="BTU"), "unknown unit"),
    (lambda e: e.update(reading_ts="not-a-time"), "unparseable timestamp"),
])
def test_bad_events_raise_with_reason(mutate, reason):
    ev = dict(V2)
    mutate(ev)
    with pytest.raises(EnvelopeError, match=reason):
        validate(ev)
