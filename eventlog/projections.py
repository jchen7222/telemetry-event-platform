"""Projections: derived state rebuilt from the immutable log, never the reverse.

The S3 log is the system of record. Everything queryable — current state,
hourly rollups, "what did we believe at 3pm Tuesday" — is a projection that
can be dropped and rebuilt from the log at any time. Applying is idempotent:
each event_id is applied exactly once no matter how many times the projector
runs, which is what makes replay safe.

Corrections are new events, not updates. A revised reading for the same
(facility, reading_ts) arrives later with a new event_id; current state picks
the latest belief, and as_of(T) reproduces the belief held at time T. That is
the audit question this platform exists to answer.
"""
from __future__ import annotations

import json
import os

import duckdb

DB_ENV = "PROJECTIONS_DB"
DEFAULT_DB = "projections.duckdb"


def db_path() -> str:
    return os.environ.get(DB_ENV, DEFAULT_DB)


def connect(path: str | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path or db_path())
    con.execute(
        """create table if not exists events_applied (
               event_id varchar primary key,
               source_id varchar,
               facility_id varchar,
               reading_ts timestamp,
               value_kwh double,
               quality_flag varchar,
               sent_at timestamp,
               ingested_at timestamp,
               pipeline_run_id varchar,
               s3_key varchar
           )"""
    )
    con.execute(
        """create table if not exists run_audit (
               pipeline_run_id varchar,
               hop varchar,
               expected bigint,
               actual bigint,
               status varchar,
               detail varchar,
               recorded_at timestamp default current_timestamp
           )"""
    )
    con.execute(
        """create or replace view current_state as
           select * exclude (rn) from (
               select *, row_number() over (
                   partition by facility_id, reading_ts order by ingested_at desc, event_id desc
               ) as rn
               from events_applied
           ) where rn = 1"""
    )
    con.execute(
        """create or replace view hourly_rollup as
           select facility_id,
                  date_trunc('hour', reading_ts) as reading_hour,
                  sum(value_kwh) as energy_kwh,
                  count(*) as readings
           from current_state
           group by 1, 2"""
    )
    return con


def apply_from_log(s3, bucket: str, run_id: str, database: str | None = None) -> dict:
    """Replay the S3 log into projections. Safe to run any number of times."""
    from .envelope import value_kwh

    con = connect(database)
    try:
        applied = skipped = log_objects = 0
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix="events/"):
            for obj in page.get("Contents", []):
                log_objects += 1
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                ev = json.loads(body)
                exists = con.execute(
                    "select count(*) from events_applied where event_id = ?",
                    [ev["event_id"]],
                ).fetchone()[0]
                if exists:
                    skipped += 1
                    continue
                con.execute(
                    "insert into events_applied values (?,?,?,?,?,?,?,?,?,?)",
                    [ev["event_id"], ev["source_id"], ev["facility_id"], ev["reading_ts"],
                     value_kwh(ev), ev["quality_flag"], ev["sent_at"], ev["ingested_at"],
                     ev["pipeline_run_id"], obj["Key"]],
                )
                applied += 1
        return {"log_objects": log_objects, "applied": applied, "skipped": skipped,
                "run_id": run_id}
    finally:
        con.close()


def as_of(when: str, database: str | None = None):
    """The belief held at `when`: latest event per (facility, reading_ts) ingested by then."""
    con = duckdb.connect(database or db_path(), read_only=True)
    try:
        return con.execute(
            """select facility_id, reading_ts, value_kwh, quality_flag, ingested_at
               from (
                   select *, row_number() over (
                       partition by facility_id, reading_ts
                       order by ingested_at desc, event_id desc
                   ) as rn
                   from events_applied
                   where ingested_at <= ?
               ) where rn = 1
               order by facility_id, reading_ts""",
            [when],
        ).fetchall()
    finally:
        con.close()


def record_hop(run_id: str, hop: str, expected: int, actual: int, detail: str = "",
               database: str | None = None) -> bool:
    """Assert one hop of the pipeline reconciles, and record the assertion."""
    ok = expected == actual
    con = connect(database)
    try:
        con.execute(
            "insert into run_audit (pipeline_run_id, hop, expected, actual, status, detail)"
            " values (?,?,?,?,?,?)",
            [run_id, hop, expected, actual, "RECONCILED" if ok else "MISMATCH", detail],
        )
    finally:
        con.close()
    return ok
