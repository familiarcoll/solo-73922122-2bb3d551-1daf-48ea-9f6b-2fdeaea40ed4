"""Persistence and cross-process coordination for OAI-PMH harvest jobs.

The coordination deliberately mirrors the rebuild machinery: one active job is
enforced by a partial unique index, claims happen under ``BEGIN IMMEDIATE``, and
every worker is fenced by an unguessable claim token plus heartbeat lease.
Catalog rows themselves are written by :mod:`app.harvest`.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import time
import uuid
from typing import Any

from .config import SETTINGS
from .db import (
    ACTIVE_HARVEST_STATES,
    begin_immediate,
    connect,
    process_start_token,
    utc_now,
)


class HarvestConflict(RuntimeError):
    """A different harvest is already active for the same configured source."""

    def __init__(self, job: dict[str, Any]) -> None:
        self.job = job
        super().__init__(f"harvest {job['job_id']} is already {job['state']}")


def harvest_job_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    applied = int(row["records_applied"])
    skipped = int(row["records_skipped"])
    withdrawn = int(row["records_withdrawn"])
    return {
        "job_id": row["id"],
        "source_url": row["source_url"],
        "metadata_prefix": row["metadata_prefix"],
        "from_datestamp": row["from_datestamp"],
        "until_datestamp": row["until_datestamp"],
        "scenario": row["scenario"],
        "state": row["state"],
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "heartbeat_at": row["heartbeat_at"],
        "resumption_token": row["resumption_token"],
        "last_identifier": row["last_identifier"],
        "requests_made": int(row["requests_made"]),
        # records_seen counts records that produced a cataloguing action
        # (apply or withdraw); skipped older/equal replays are counted alone.
        "records_seen": int(row["records_seen"]),
        "records_applied": applied,
        "records_skipped": skipped,
        "records_withdrawn": withdrawn,
        "records_processed_total": applied + skipped + withdrawn,
        "restarts": int(row["restarts"]),
        "owner_host": row["owner_host"],
        "owner_pid": row["owner_pid"],
        "message": row["message"],
        "error": row["error"],
        "active": row["state"] in ACTIVE_HARVEST_STATES,
    }


def get_harvest_job(job_id: str) -> dict[str, Any] | None:
    conn = connect()
    try:
        row = conn.execute(
            "SELECT * FROM harvest_jobs WHERE id = ?", (job_id,)
        ).fetchone()
        return harvest_job_to_dict(row)
    finally:
        conn.close()


def list_harvest_jobs(limit: int = 20) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT * FROM harvest_jobs ORDER BY created_epoch DESC LIMIT ?",
            (max(1, min(int(limit), 100)),),
        ).fetchall()
        return [harvest_job_to_dict(row) for row in rows]  # type: ignore[list-item]
    finally:
        conn.close()


def get_active_harvest() -> dict[str, Any] | None:
    conn = connect()
    try:
        placeholders = ", ".join("?" for _ in ACTIVE_HARVEST_STATES)
        row = conn.execute(
            f"SELECT * FROM harvest_jobs WHERE state IN ({placeholders}) "
            "ORDER BY created_epoch DESC LIMIT 1",
            ACTIVE_HARVEST_STATES,
        ).fetchone()
        return harvest_job_to_dict(row)
    finally:
        conn.close()


def _harvest_is_stale(job: sqlite3.Row, now_epoch: float) -> tuple[bool, str]:
    host = socket.gethostname()
    if job["owner_host"] == host and job["owner_pid"]:
        pid = int(job["owner_pid"])
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True, "harvest owner process is no longer running"
        except PermissionError:
            pass
        else:
            if job["owner_proc_start"] and process_start_token(pid) != job["owner_proc_start"]:
                return True, "harvest owner PID was reused by a restarted process"
    heartbeat = job["heartbeat_at"] or job["created_epoch"]
    if now_epoch - float(heartbeat) > SETTINGS.harvest_lease_seconds:
        return True, "harvest heartbeat lease expired"
    return False, ""


def _resumable_match(
    row: sqlite3.Row,
    *,
    from_datestamp: str | None,
    until_datestamp: str,
    scenario: str | None,
) -> bool:
    return (
        row["state"] == "interrupted"
        and row["until_datestamp"] == until_datestamp
        and (row["from_datestamp"] or None) == from_datestamp
        and (row["scenario"] or None) == scenario
    )


def claim_harvest(
    *,
    source_url: str,
    metadata_prefix: str,
    from_datestamp: str | None,
    until_datestamp: str,
    scenario: str | None,
) -> dict[str, Any]:
    """Claim the single harvest slot, reuse the active job or resume an interrupted one.

    Returns a dict with ``created``, ``resumed``, ``reused_existing`` flags and
    the ``worker_token`` when this caller owns the (re)claimed job and must
    start the worker.
    """

    conn = connect()
    try:
        begin_immediate(conn)
        now_epoch = time.time()
        placeholders = ", ".join("?" for _ in ACTIVE_HARVEST_STATES)
        running = conn.execute(
            f"SELECT * FROM harvest_jobs WHERE state IN ({placeholders}) "
            "ORDER BY created_epoch DESC LIMIT 1",
            ACTIVE_HARVEST_STATES,
        ).fetchone()

        if running is not None:
            stale, reason = _harvest_is_stale(running, now_epoch)
            if stale:
                conn.execute(
                    """
                    UPDATE harvest_jobs
                       SET state = 'interrupted', finished_at = ?, heartbeat_at = ?,
                           error = ?, message = 'reclaimed after lease expiry'
                     WHERE id = ?
                    """,
                    (utc_now(), now_epoch, reason, running["id"]),
                )
            else:
                conn.commit()
                job = harvest_job_to_dict(running)
                return {
                    "job": job,
                    "created": False,
                    "resumed": False,
                    "reused_existing": True,
                    "worker_token": None,
                }

        # Prefer resuming an interrupted job for the exact same interval so a
        # restart/stream break continues from the persisted resumption token.
        interrupted = conn.execute(
            """
            SELECT * FROM harvest_jobs
             WHERE state = 'interrupted'
               AND source_url = ? AND metadata_prefix = ?
               AND until_datestamp = ?
               AND COALESCE(from_datestamp, '') = COALESCE(?, '')
               AND COALESCE(scenario, '') = COALESCE(?, '')
             ORDER BY created_epoch DESC LIMIT 1
            """,
            (source_url, metadata_prefix, until_datestamp, from_datestamp, scenario),
        ).fetchone()

        if interrupted is not None:
            claim_token = str(uuid.uuid4())
            conn.execute(
                """
                UPDATE harvest_jobs
                   SET state = 'queued', claim_token = ?, owner_host = ?, owner_pid = ?,
                       owner_proc_start = ?, heartbeat_at = ?, finished_at = NULL,
                       error = NULL, message = 'resuming interrupted harvest'
                 WHERE id = ? AND state = 'interrupted'
                """,
                (
                    claim_token,
                    socket.gethostname(),
                    os.getpid(),
                    process_start_token(os.getpid()),
                    now_epoch,
                    interrupted["id"],
                ),
            )
            job_row = conn.execute(
                "SELECT * FROM harvest_jobs WHERE id = ?", (interrupted["id"],)
            ).fetchone()
            conn.commit()
            return {
                "job": harvest_job_to_dict(job_row),
                "created": False,
                "resumed": True,
                "reused_existing": False,
                "worker_token": claim_token,
            }

        job_id = str(uuid.uuid4())
        claim_token = str(uuid.uuid4())
        now = utc_now()
        conn.execute(
            """
            INSERT INTO harvest_jobs
                (id, source_url, metadata_prefix, from_datestamp, until_datestamp,
                 scenario, state, claim_token, owner_host, owner_pid,
                 owner_proc_start, created_at, created_epoch, heartbeat_at, message)
            VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                source_url,
                metadata_prefix,
                from_datestamp,
                until_datestamp,
                scenario,
                claim_token,
                socket.gethostname(),
                os.getpid(),
                process_start_token(os.getpid()),
                now,
                now_epoch,
                now_epoch,
                "queued for OAI-PMH harvest",
            ),
        )
        job_row = conn.execute(
            "SELECT * FROM harvest_jobs WHERE id = ?", (job_id,)
        ).fetchone()
        conn.commit()
        return {
            "job": harvest_job_to_dict(job_row),
            "created": True,
            "resumed": False,
            "reused_existing": False,
            "worker_token": claim_token,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def harvest_is_owned(job_id: str, claim_token: str) -> bool:
    conn = connect()
    try:
        row = conn.execute(
            "SELECT state, claim_token FROM harvest_jobs WHERE id = ?", (job_id,)
        ).fetchone()
        return (
            row is not None
            and row["claim_token"] == claim_token
            and row["state"] in ACTIVE_HARVEST_STATES
        )
    finally:
        conn.close()


def mark_harvest_progress(
    job_id: str,
    claim_token: str,
    *,
    state: str | None = None,
    started: bool = False,
    resumption_token: str | None = None,
    last_identifier: str | None = None,
    requests_made: int | None = None,
    records_seen: int | None = None,
    records_applied: int | None = None,
    records_skipped: int | None = None,
    records_withdrawn: int | None = None,
    restarts: int | None = None,
    message: str | None = None,
    clear_resumption_token: bool = False,
) -> bool:
    """Update heartbeat/counters only while this worker still owns the job."""

    conn = connect()
    try:
        begin_immediate(conn)
        assignments = ["heartbeat_at = ?"]
        params: list[Any] = [time.time()]
        if state is not None:
            assignments.append("state = ?")
            params.append(state)
        if started:
            assignments.append("started_at = COALESCE(started_at, ?)")
            params.append(utc_now())
        if clear_resumption_token:
            assignments.append("resumption_token = NULL")
        elif resumption_token is not None:
            assignments.append("resumption_token = ?")
            params.append(resumption_token)
        if last_identifier is not None:
            assignments.append("last_identifier = ?")
            params.append(last_identifier)
        if requests_made is not None:
            assignments.append("requests_made = ?")
            params.append(int(requests_made))
        if records_seen is not None:
            assignments.append("records_seen = ?")
            params.append(int(records_seen))
        if records_applied is not None:
            assignments.append("records_applied = ?")
            params.append(int(records_applied))
        if records_skipped is not None:
            assignments.append("records_skipped = ?")
            params.append(int(records_skipped))
        if records_withdrawn is not None:
            assignments.append("records_withdrawn = ?")
            params.append(int(records_withdrawn))
        if restarts is not None:
            assignments.append("restarts = ?")
            params.append(int(restarts))
        if message is not None:
            assignments.append("message = ?")
            params.append(message)

        placeholders = ", ".join("?" for _ in ACTIVE_HARVEST_STATES)
        params.extend([job_id, claim_token, *ACTIVE_HARVEST_STATES])
        cur = conn.execute(
            f"""
            UPDATE harvest_jobs SET {', '.join(assignments)}
             WHERE id = ? AND claim_token = ?
               AND state IN ({placeholders})
            """,
            params,
        )
        owned = cur.rowcount > 0
        conn.commit()
        return owned
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def finish_harvest(
    job_id: str,
    claim_token: str,
    *,
    state: str,
    error: str | None,
    message: str | None = None,
) -> bool:
    """Move a job to a terminal state, fenced by the claim token."""

    conn = connect()
    try:
        begin_immediate(conn)
        placeholders = ", ".join("?" for _ in ACTIVE_HARVEST_STATES)
        cur = conn.execute(
            f"""
            UPDATE harvest_jobs
               SET state = ?, finished_at = ?, heartbeat_at = ?,
                   error = ?, message = COALESCE(?, message)
             WHERE id = ? AND claim_token = ?
               AND state IN ({placeholders})
            """,
            (state, utc_now(), time.time(), error, message, job_id, claim_token,
             *ACTIVE_HARVEST_STATES),
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_harvest_errors(job_id: str, limit: int = 50) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT * FROM harvest_errors WHERE job_id = ? ORDER BY id DESC LIMIT ?",
            (job_id, max(1, min(int(limit), 500))),
        ).fetchall()
        return [
            {
                "page_id": row["page_id"],
                "datestamp": row["datestamp"],
                "kind": row["kind"],
                "detail": row["detail"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]
    finally:
        conn.close()


def get_catalog(page_id: str) -> dict[str, Any] | None:
    conn = connect()
    try:
        conn.execute("BEGIN")
        row = conn.execute(
            "SELECT * FROM catalog WHERE page_id = ?", (page_id,)
        ).fetchone()
        conn.execute("COMMIT")
        if row is None:
            return None
        import json

        return {
            "page_id": row["page_id"],
            "status": row["status"],
            "titles": json.loads(row["titles"]),
            "creators": json.loads(row["creators"]),
            "source": row["source"],
            "record_datestamp": row["record_datestamp"],
            "first_seen_at": row["first_seen_at"],
            "updated_at": row["updated_at"],
            "harvest_job_id": row["harvest_job_id"],
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
