# telemetry-event-platform

**An immutable event log for industrial telemetry: Kinesis → Lambda → S3, with
projections that can always answer "what did we believe, and when?"**

![ci](https://github.com/jchen7222/telemetry-event-platform/actions/workflows/ci.yml/badge.svg)

Real AWS Lambda handlers, runnable end-to-end with zero credentials: the demo and CI
execute the same handler code against [moto](https://github.com/getmoto/moto)-mocked
Kinesis and S3, in-process. `infra/template.yaml` documents the identical deployed shape.

```bash
pip install -r requirements.txt
make all     # synthetic gateways → Kinesis → Lambda → S3 log → projections → dashboard → tests
```

```
wave 1: produced=487 landed=480 duplicates=1 quarantined=6 projected=480
wave 2: corrections landed=3 projected=3 (previously applied skipped=480)

the audit question — same reading, two beliefs:
  CHM-1 2024-03-04 09:00:00: believed 772.4 kWh then, believe 669.4 kWh now — both reproducible

hops RECONCILED; snapshot -> ops_snapshot.json
```

## Why this exists

I led the production version of this platform at a government information center:
energy-consumption telemetry from 1,000+ industrial facilities across 24 industries,
7,200+ IoT readings per hour at peak, where state-dependent legacy processes were
replaced with an immutable event log on AWS (Lambda, Kinesis, S3). That code is
proprietary; this repository is a from-scratch reimplementation of the architecture
on synthetic data. It is the streaming half of a pair — the warehouse half
(configuration-driven ingestion, dbt, versioned model contracts on Snowflake/DuckDB)
is [template-driven-ingestion](https://github.com/jchen7222/template-driven-ingestion),
and the event-sourcing modeling ideas also appear in
[supply-chain-event-platform](https://github.com/jchen7222/supply-chain-event-platform).

## Architecture

```
 gateways (v1 and v2 envelopes, kWh/MWh/MJ)
      │  put_records
      ▼
 Kinesis stream ──► ingest Lambda (eventlog/ingest.py)
                       │  validate / upgrade v1→v2
                       ├─► s3://…/events/<source>/<date>/<event_id>.json   immutable log
                       ├─► s3://…/quarantine/<run>/…                       bad events + reason
                       └─► hop counts: records == landed + duplicates + quarantined
                                                          │
                            projection Lambda (eventlog/projections.py)
                                                          ▼
                              events_applied (exactly-once) ─► current_state
                                                             ─► hourly_rollup
                                                             ─► as_of(T)      ← the audit query
                                                          │
                              run_audit (per-hop reconciliation, every run)
                                                          │
                              ops dashboard (eventlog/dashboard.py → dashboard.html)
```

## The rules the platform enforces

**The log is append-only.** Every valid event becomes one S3 object whose key is derived
from the event itself. Corrections are *new events* (new `event_id`, same reading), never
updates. The bucket has versioning enabled in the template as a guard even against
operator error.

**Duplicates collapse; they don't accumulate.** Kinesis delivers at-least-once. Because
the object key is content-derived, a redelivered event maps to the same key — the demo
injects an exact duplicate and the audit shows `duplicates=1`, not a double-counted reading.

**Two envelope versions are live at once.** Field gateways don't upgrade on the platform's
schedule. `schema_version` is explicit on every event; v1 upgrades to canonical v2 at the
edge; unknown versions quarantine with a reason instead of crashing the batch.

**Quarantine, not exceptions.** Malformed JSON, negative energy, unknown units, clock
faults — the demo feed includes them all. Each lands in `quarantine/` with the reason and
the original payload, and the batch keeps going.

**Every hop reconciles, every run.** `produced == fetched`, `fetched == landed +
duplicates + quarantined`, `log == applied`. Each assertion is recorded in `run_audit`
with the run's `pipeline_run_id`; a mismatch fails the run (and CI). Any figure in any
projection traces back to the file, batch, and load that produced it.

**Projections are disposable; the log is not.** `current_state`, `hourly_rollup`, and
`as_of(T)` are all rebuilt from the log; applying is exactly-once per `event_id`, so
replay is always safe. `as_of(T)` answers the auditor's question: the demo revises three
readings in a second wave, then shows the belief *before* and *after* — both reproducible.

**Operations is self-service.** `make dashboard` renders `dashboard.html`: hop
reconciliation, per-facility state, and every quarantined event diagnosed against
`ops/patterns.yml` — a registry mapping failure signatures to known root-cause patterns
and the owning team's action. Encoding what each failure has meant before is what lets
other teams resolve their own incidents without a data-engineering ticket.

## Repo layout

```
eventlog/
  envelope.py       schema versions, validation, v1→v2 upgrade, unit normalization
  ingest.py         the ingest Lambda handler (Kinesis batch → S3 log + quarantine)
  projections.py    exactly-once apply, current_state / hourly_rollup / as_of, run_audit
  dashboard.py      static self-service ops dashboard
ops/patterns.yml    root-cause pattern registry (failure signature → diagnosis → action)
demo/run_pipeline.py  end-to-end run against moto-mocked AWS, two waves + audit demo
scripts/make_devices.py  deterministic synthetic gateway traffic (seed=11)
infra/template.yaml  AWS SAM template: stream, versioned log bucket, DLQ, both functions
tests/               envelope, ingest (moto), projections/as-of, dashboard — 16 tests
.github/workflows/ci.yml  the same make targets on every push; dashboard uploaded as artifact
```

All sample data is synthetic. MIT license.
