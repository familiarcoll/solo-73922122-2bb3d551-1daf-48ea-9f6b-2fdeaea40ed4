"""OAI-PMH harvest worker: transactional catalog writes and restartable paging.

Correctness rules implemented here:

* The network wait for a ListRecords response never happens inside a database
  transaction, so OCR corrections keep committing while a harvest is in flight.
* Each record's full-page catalog change is committed in one short
  ``BEGIN IMMEDIATE`` transaction together with the resumption cursor, so a
  crash/stream break/bad XML response can never leave a half-applied page.
* Deleted OAI records only create or refresh a cataloguing tombstone; the
  canonical OCR ``pages`` table is never touched by harvesting.
* Every decision is driven by the UTC-second record datestamp: older records
  are skipped, equal datestamp with identical content is a no-op, and equal
  datestamp with different content fails the whole job with a page-level error.
  A tombstone keeps its deletion time and can never be revived by an old record.
* A bad/expired resumption token restarts the *original* interval; the
  datestamp arbitration above makes the replay idempotent.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from typing import Any

from .db import begin_immediate, connect, utc_now
from .harvest_store import (
    finish_harvest,
    harvest_is_owned,
    mark_harvest_progress,
)
from .oai import (
    HarvestError,
    HarvestParseError,
    HarvestProtocolError,
    HarvestTransportError,
    ListRecordsResponse,
    OaiRecord,
    list_records,
)

logger = logging.getLogger("archive.harvest")

_local_jobs: dict[str, tuple[threading.Event, str]] = {}
_registry_lock = threading.Lock()


class HarvestCancelled(Exception):
    pass


class HarvestConflictError(Exception):
    """Deterministic same-datestamp/different-content contradiction."""

    def __init__(self, page_id: str, detail: str) -> None:
        self.page_id = page_id
        self.detail = detail
        super().__init__(detail)


def start_harvest_thread(
    *,
    job_id: str,
    claim_token: str,
    source_url: str,
    metadata_prefix: str,
    from_datestamp: str | None,
    until_datestamp: str,
    scenario: str | None,
) -> threading.Thread:
    stop_event = threading.Event()
    with _registry_lock:
        _local_jobs[job_id] = (stop_event, claim_token)

    thread = threading.Thread(
        target=_run_worker,
        kwargs={
            "job_id": job_id,
            "claim_token": claim_token,
            "source_url": source_url,
            "metadata_prefix": metadata_prefix,
            "from_datestamp": from_datestamp,
            "until_datestamp": until_datestamp,
            "scenario": scenario,
            "stop_event": stop_event,
        },
        name=f"harvest-{job_id[:8]}",
        daemon=True,
    )
    thread.start()
    return thread


def cancel_local_harvests() -> None:
    with _registry_lock:
        entries = list(_local_jobs.items())
        for _, (event, _token) in entries:
            event.set()


def _check_owned(job_id: str, claim_token: str, stop_event: threading.Event) -> None:
    if stop_event.is_set() or not harvest_is_owned(job_id, claim_token):
        raise HarvestCancelled("harvest job is no longer owned by this process")


def _record_harvest_error(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    page_id: str,
    datestamp: str | None,
    kind: str,
    detail: str,
) -> None:
    conn.execute(
        """
        INSERT INTO harvest_errors(job_id, page_id, datestamp, kind, detail, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (job_id, page_id, datestamp, kind, detail, utc_now()),
    )


def _apply_record(
    conn: sqlite3.Connection,
    *,
    record: OaiRecord,
    source_url: str,
    job_id: str,
) -> str:
    """Apply one record inside an open write transaction.

    Returns one of ``applied``, ``withdrawn`` or ``skipped``.
    Raises :class:`HarvestConflictError` on a same-timestamp contradiction.
    """

    now = utc_now()
    existing = conn.execute(
        "SELECT * FROM catalog WHERE page_id = ?", (record.identifier,)
    ).fetchone()

    if existing is not None:
        old_datestamp = str(existing["record_datestamp"])
        if record.datestamp < old_datestamp:
            # Older records must never change or revive anything.
            return "skipped"
        if record.datestamp == old_datestamp:
            if record.deleted and existing["status"] == "withdrawn":
                return "skipped"
            old_titles = json.loads(existing["titles"])
            old_creators = json.loads(existing["creators"])
            if (
                not record.deleted
                and existing["status"] == "active"
                and list(record.titles) == old_titles
                and list(record.creators) == old_creators
            ):
                # Same time, same content: an idempotent replay.
                return "skipped"
            detail = (
                f"record {record.identifier!r} at datestamp {record.datestamp} "
                f"contradicts the stored {existing['status']} record at the same "
                f"datestamp {old_datestamp}"
            )
            raise HarvestConflictError(record.identifier, detail)

    if record.deleted:
        if existing is None:
            conn.execute(
                """
                INSERT INTO catalog(page_id, titles, creators, source, record_datestamp,
                                    status, first_seen_at, updated_at, harvest_job_id)
                VALUES (?, '[]', '[]', ?, ?, 'withdrawn', ?, ?, ?)
                """,
                (record.identifier, source_url, record.datestamp, now, now, job_id),
            )
        else:
            # Preserve the original first_seen time; refresh the deletion time.
            conn.execute(
                """
                UPDATE catalog
                   SET titles = '[]', creators = '[]', source = ?,
                       record_datestamp = ?, status = 'withdrawn',
                       updated_at = ?, harvest_job_id = ?
                 WHERE page_id = ?
                """,
                (source_url, record.datestamp, now, job_id, record.identifier),
            )
        return "withdrawn"

    conn.execute(
        """
        INSERT INTO catalog(page_id, titles, creators, source, record_datestamp,
                            status, first_seen_at, updated_at, harvest_job_id)
        VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?)
        ON CONFLICT(page_id) DO UPDATE SET
            titles = excluded.titles,
            creators = excluded.creators,
            source = excluded.source,
            record_datestamp = excluded.record_datestamp,
            status = 'active',
            updated_at = excluded.updated_at,
            harvest_job_id = excluded.harvest_job_id
        """,
        (
            record.identifier,
            json.dumps(list(record.titles), ensure_ascii=False),
            json.dumps(list(record.creators), ensure_ascii=False),
            source_url,
            record.datestamp,
            now,
            now,
            job_id,
        ),
    )
    return "applied"


def _persist_harvest_error(
    *,
    job_id: str,
    page_id: str,
    datestamp: str | None,
    kind: str,
    detail: str,
) -> None:
    """Record a page-level error in its own transaction after a rollback."""

    conn = connect()
    try:
        begin_immediate(conn)
        _record_harvest_error(
            conn,
            job_id=job_id,
            page_id=page_id,
            datestamp=datestamp,
            kind=kind,
            detail=detail,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _commit_page(
    *,
    job_id: str,
    claim_token: str,
    record: OaiRecord,
    source_url: str,
    next_token: str | None,
) -> dict[str, int]:
    """One short write transaction per full-page change (plus cursor).

    Returns the post-commit counters as durably stored in the database, so the
    worker never reports in-memory numbers that a token restart invalidated.
    """

    conn = connect()
    try:
        begin_immediate(conn)
        outcome = _apply_record(
            conn, record=record, source_url=source_url, job_id=job_id
        )
        # The resumption cursor moves in the same transaction as the page
        # change, so no committed page can ever be re-fetched after a crash.
        # records_seen counts only records that produced a catalog action;
        # records absorbed by datestamp arbitration (older/equal replays)
        # move records_skipped alone, so a token-driven re-harvest cannot
        # inflate the seen count.
        if outcome == "applied":
            counter_sql = (
                "records_seen = records_seen + 1, "
                "records_applied = records_applied + 1"
            )
        elif outcome == "withdrawn":
            counter_sql = (
                "records_seen = records_seen + 1, "
                "records_withdrawn = records_withdrawn + 1"
            )
        else:
            counter_sql = "records_skipped = records_skipped + 1"
        conn.execute(
            f"UPDATE harvest_jobs SET resumption_token = ?, last_identifier = ?, "
            f"{counter_sql} WHERE id = ?",
            (next_token, record.identifier, job_id),
        )
        row = conn.execute(
            "SELECT records_seen, records_applied, records_skipped, "
            "records_withdrawn FROM harvest_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        conn.commit()
        return {
            "records_seen": int(row["records_seen"]),
            "records_applied": int(row["records_applied"]),
            "records_skipped": int(row["records_skipped"]),
            "records_withdrawn": int(row["records_withdrawn"]),
        }
    except HarvestConflictError:
        # Roll back the whole page transaction first (no half page, cursor
        # unchanged), then persist the page error durably on its own.
        conn.rollback()
        _persist_harvest_error(
            job_id=job_id,
            page_id=record.identifier,
            datestamp=record.datestamp,
            kind="datestamp_conflict",
            detail=(
                f"record {record.identifier!r} at datestamp {record.datestamp} "
                "contradicts the stored record at the same datestamp"
            ),
        )
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _fetch_page(
    *,
    source_url: str,
    metadata_prefix: str,
    from_datestamp: str | None,
    until_datestamp: str,
    token: str | None,
    scenario: str | None,
    requests_made: int,
) -> ListRecordsResponse:
    """Network call performed without any open database transaction."""

    extra_params: dict[str, str] | None = None
    if token is None and scenario:
        # The demo source understands this OAI-ignored query parameter; a real
        # OAI server simply ignores the unknown argument.
        extra_params = {"demo_scenario": scenario}
    return list_records(
        source_url,
        metadata_prefix=metadata_prefix,
        from_datestamp=from_datestamp if token is None else None,
        until_datestamp=until_datestamp if token is None else None,
        resumption_token=token,
        extra_params=extra_params,
    )


def _persist_cursor_only(
    *,
    job_id: str,
    token: str | None,
    requests_made: int,
    restarts: int,
) -> None:
    conn = connect()
    try:
        begin_immediate(conn)
        if token is None:
            conn.execute(
                "UPDATE harvest_jobs SET resumption_token = NULL, requests_made = ?, "
                "restarts = ? WHERE id = ?",
                (requests_made, restarts, job_id),
            )
        else:
            conn.execute(
                "UPDATE harvest_jobs SET resumption_token = ?, requests_made = ?, "
                "restarts = ? WHERE id = ?",
                (token, requests_made, restarts, job_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _run_worker(
    *,
    job_id: str,
    claim_token: str,
    source_url: str,
    metadata_prefix: str,
    from_datestamp: str | None,
    until_datestamp: str,
    scenario: str | None,
    stop_event: threading.Event,
) -> None:
    restarts = 0
    requests_made = 0
    # Resume from the cursor and the counters persisted by the previous
    # (interrupted) attempt.  All counters live in the database so a token
    # expiry that restarts the interval cannot double-count or lose rows.
    conn = connect()
    try:
        row = conn.execute(
            "SELECT resumption_token, restarts, requests_made FROM harvest_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        token: str | None = row["resumption_token"] if row else None
        if row is not None:
            restarts = int(row["restarts"])
            requests_made = int(row["requests_made"])
    finally:
        conn.close()

    try:
        mark_harvest_progress(
            job_id,
            claim_token,
            state="running",
            started=True,
            message="starting OAI-PMH ListRecords harvest",
        )
        _check_owned(job_id, claim_token, stop_event)

        while True:
            _check_owned(job_id, claim_token, stop_event)
            mark_harvest_progress(
                job_id,
                claim_token,
                message=(
                    f"requesting ListRecords page {requests_made + 1}"
                    + (" via resumption token" if token else " from interval start")
                ),
            )
            try:
                # No DB transaction is open while waiting on the network.
                response = _fetch_page(
                    source_url=source_url,
                    metadata_prefix=metadata_prefix,
                    from_datestamp=from_datestamp,
                    until_datestamp=until_datestamp,
                    token=token,
                    scenario=scenario,
                    requests_made=requests_made,
                )
            except HarvestProtocolError as exc:
                requests_made += 1
                mark_harvest_progress(
                    job_id,
                    claim_token,
                    requests_made=requests_made,
                    message=f"OAI protocol response ({exc.code}) on request {requests_made}",
                )
                if token is not None and exc.code in {
                    "badResumptionToken",
                    "expired",
                }:
                    # Token expired (or never existed after a source restart):
                    # re-harvest the original closed interval and let the
                    # datestamp arbitration deduplicate every replay.
                    logger.warning(
                        "harvest %s resumption token rejected (%s); restarting interval",
                        job_id,
                        exc.code,
                    )
                    token = None
                    restarts += 1
                    _persist_cursor_only(
                        job_id=job_id, token=None,
                        requests_made=requests_made, restarts=restarts,
                    )
                    mark_harvest_progress(
                        job_id,
                        claim_token,
                        restarts=restarts,
                        message=f"resumption token {exc.code}; re-harvesting original interval",
                    )
                    continue
                raise
            requests_made += 1

            latest_counters: dict[str, int] | None = None
            for record in response.records:
                _check_owned(job_id, claim_token, stop_event)
                latest_counters = _commit_page(
                    job_id=job_id,
                    claim_token=claim_token,
                    record=record,
                    source_url=source_url,
                    # Only expose the next cursor after every record of this
                    # page has been committed; the last record's transaction
                    # stores it together with its page change.
                    next_token=response.resumption_token,
                )

            if not response.records:
                # An empty final page still clears any stale cursor so a
                # finished job never presents a usable resumption token.
                _persist_cursor_only(
                    job_id=job_id,
                    token=response.resumption_token,
                    requests_made=requests_made,
                    restarts=restarts,
                )

            progress_kwargs: dict[str, Any] = {
                "requests_made": requests_made,
                "message": "processing ListRecords pages",
            }
            if latest_counters is not None:
                progress_kwargs.update(latest_counters)
                progress_kwargs["message"] = (
                    f"processed {latest_counters['records_seen']} records so far"
                )
            mark_harvest_progress(job_id, claim_token, **progress_kwargs)

            if not response.resumption_token:
                break
            token = response.resumption_token

        final_conn = connect()
        try:
            final_row = final_conn.execute(
                "SELECT records_seen, records_applied, records_skipped, "
                "records_withdrawn FROM harvest_jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
        finally:
            final_conn.close()
        finish_harvest(
            job_id,
            claim_token,
            state="succeeded",
            error=None,
            message=(
                f"harvest complete: {int(final_row['records_applied'])} applied, "
                f"{int(final_row['records_withdrawn'])} withdrawn, "
                f"{int(final_row['records_skipped'])} skipped across {requests_made} requests"
                + (f", {restarts} token restart(s)" if restarts else "")
            ),
        )
        logger.info("harvest %s succeeded", job_id)
    except HarvestCancelled as exc:
        logger.info("harvest %s interrupted: %s", job_id, exc)
        finish_harvest(
            job_id,
            claim_token,
            state="interrupted",
            error=str(exc) or "harvest interrupted",
            message="resume by retriggering the same interval",
        )
    except HarvestConflictError as exc:
        logger.error("harvest %s failed on page %s: %s", job_id, exc.page_id, exc.detail)
        finish_harvest(
            job_id,
            claim_token,
            state="failed",
            error=f"datestamp conflict on page {exc.page_id}: {exc.detail}",
            message="fix the contradictory source record, then retrigger the interval",
        )
    except (HarvestTransportError, HarvestParseError) as exc:
        # Stream interruption or bad XML: nothing partial was committed.  The
        # job is interrupted and a retrigger resumes from the saved cursor.
        logger.warning("harvest %s interrupted by source failure: %s", job_id, exc)
        finish_harvest(
            job_id,
            claim_token,
            state="interrupted",
            error=f"{type(exc).__name__}: {exc}",
            message="resume by retriggering the same interval",
        )
    except HarvestError as exc:
        logger.exception("harvest %s failed with a protocol error", job_id)
        finish_harvest(
            job_id,
            claim_token,
            state="failed",
            error=f"{type(exc).__name__}: {exc}",
            message="unrecoverable OAI protocol error",
        )
    except Exception as exc:  # pragma: no cover - defensive catch-all
        logger.exception("harvest %s failed unexpectedly", job_id)
        finish_harvest(
            job_id,
            claim_token,
            state="failed",
            error=f"{type(exc).__name__}: {exc}",
            message="unexpected harvester failure",
        )
    finally:
        with _registry_lock:
            _local_jobs.pop(job_id, None)


def harvest_progress(job: dict[str, Any]) -> dict[str, Any]:
    """Attach the recent page errors to a serialized job for the status API."""

    from .harvest_store import list_harvest_errors

    payload: dict[str, Any] = dict(job)
    payload["recent_errors"] = list_harvest_errors(job["job_id"])
    return payload
