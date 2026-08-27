"""Render the self-service operations dashboard (static HTML, no dependencies).

Input: ops_snapshot.json (hop counts + quarantine records from the last run)
and projections.duckdb (per-facility state). Output: dashboard.html — the page
an operations team opens instead of filing a data-engineering ticket.
"""
from __future__ import annotations

import html
import json
import re

import duckdb
import yaml

from .projections import db_path

SNAPSHOT = "ops_snapshot.json"
PATTERNS = "ops/patterns.yml"
OUT = "dashboard.html"


def diagnose(reason: str, patterns: list[dict]) -> dict:
    for p in patterns:
        if re.search(p["match"], reason):
            return p
    return {"pattern": "Unrecognized failure", "likely_cause": "New failure mode",
            "action": "Escalate to platform team; add a pattern once diagnosed"}


def _table(headers: list[str], rows: list[list]) -> str:
    head = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in row) + "</tr>"
        for row in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def render(snapshot_path: str = SNAPSHOT, patterns_path: str = PATTERNS,
           out_path: str = OUT, database: str | None = None) -> str:
    snap = json.load(open(snapshot_path))
    patterns = yaml.safe_load(open(patterns_path))["patterns"]

    con = duckdb.connect(database or db_path(), read_only=True)
    hops = con.execute(
        "select hop, expected, actual, status, detail from run_audit"
        " where pipeline_run_id = ? order by recorded_at", [snap["run_id"]]).fetchall()
    facilities = con.execute(
        """select facility_id,
                  count(*) as readings,
                  round(sum(value_kwh), 1) as total_kwh,
                  max(reading_ts) as latest_reading,
                  sum(case when quality_flag = 'REVISED' then 1 else 0 end) as revisions
           from current_state group by 1 order by 1""").fetchall()
    superseded = con.execute(
        "select count(*) from events_applied where event_id not in"
        " (select event_id from current_state)").fetchone()[0]
    con.close()

    w1 = snap["wave1"]
    cards = [
        ("Events landed", w1["landed"] + snap["wave2"]["landed"]),
        ("Duplicates collapsed", w1["duplicates"]),
        ("Quarantined", w1["quarantined"]),
        ("Corrections applied", superseded),
        ("Hops reconciled", f"{sum(1 for h in hops if h[3] == 'RECONCILED')}/{len(hops)}"),
    ]
    card_html = "".join(
        f'<div class="card"><div class="n">{html.escape(str(v))}</div>'
        f'<div class="l">{html.escape(k)}</div></div>' for k, v in cards)

    qrows = []
    for q in snap["quarantine"]:
        d = diagnose(q["reason"], patterns)
        qrows.append([q["reason"], d["pattern"], d["likely_cause"], d["action"]])

    page = f"""<!doctype html><meta charset="utf-8">
<title>Telemetry platform — operations</title>
<style>
 body{{font:14px/1.5 system-ui,sans-serif;margin:2rem auto;max-width:70rem;color:#1a2733}}
 h1{{font-size:1.4rem}} h2{{font-size:1.05rem;margin-top:2rem}}
 .cards{{display:flex;gap:1rem;flex-wrap:wrap}}
 .card{{border:1px solid #d6dee6;border-radius:8px;padding:.8rem 1.2rem;min-width:9rem}}
 .n{{font-size:1.6rem;font-weight:700}} .l{{color:#5b6b7a;font-size:.85rem}}
 table{{border-collapse:collapse;width:100%;margin-top:.5rem}}
 th,td{{border:1px solid #d6dee6;padding:.35rem .6rem;text-align:left;vertical-align:top}}
 th{{background:#f2f5f8}} .ok{{color:#0a7d33;font-weight:600}}
</style>
<h1>Telemetry event platform — operations dashboard</h1>
<p>Pipeline run <code>{html.escape(snap["run_id"])}</code>. Every number below traces to
the immutable event log; nothing on this page requires a data-engineering ticket to explain.</p>
<div class="cards">{card_html}</div>
<h2>Hop reconciliation</h2>
{_table(["Hop", "Expected", "Actual", "Status", "What it asserts"], hops)}
<h2>Quarantine — diagnosed against the root-cause pattern registry</h2>
{_table(["Reason", "Known pattern", "Likely cause", "Action for the owning team"], qrows)}
<h2>Facilities (current state)</h2>
{_table(["Facility", "Readings", "Total kWh", "Latest reading", "Revisions"], facilities)}
<p>Belief at any past moment is reproducible: as-of pivot for this run was
<code>{html.escape(snap["as_of_pivot"])}</code> — see <code>eventlog.projections.as_of</code>.</p>
"""
    with open(out_path, "w") as f:
        f.write(page)
    return out_path


if __name__ == "__main__":
    print("wrote", render())
