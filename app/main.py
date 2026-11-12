"""FastAPI application: OCR page writes, accent-aware search, live rebuild and OAI-PMH harvesting."""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Body, FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, ConfigDict, Field

from .accents import AccentPolicy
from .archive import delete_page, get_page, search, upsert_page
from .config import SETTINGS
from .db import claim_rebuild, connect, fail_job, get_job, get_status, init_db, utc_now
from .harvest import (
    cancel_local_harvests,
    harvest_progress,
    start_harvest_thread,
)
from .harvest_store import (
    claim_harvest,
    finish_harvest,
    get_active_harvest,
    get_catalog,
    get_harvest_job,
    list_harvest_jobs,
)
from .oai import format_utc_second, parse_utc_second
from .rebuild import cancel_local_rebuilds, start_rebuild_thread


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(process)d %(message)s",
)
logger = logging.getLogger("archive.api")


APP_DESCRIPTION = """
Accent-aware OCR archive search backed by **SQLite FTS5**, with transactional
page corrections and an online, background index rebuild.

### Index generations and accent policy

* Every index generation has its own FTS5 table.
* `keep` indexes accented Latin letters exactly (`café` != `cafe`). This is the
  initial policy.
* `fold` removes combining accents from Latin letters (`café` == `cafe`,
  `señor` == `senor`, `naïve` == `naive`).
* `POST /api/index/rebuild` creates a **new generation** in the background.
  The old generation remains active, and page writes/deletes continue to
  commit normally. Writes are never blocked by a lock covering the scan.
* The new generation snapshots the canonical pages and then replays every
  committed write from the durable change log. The active pointer is switched
  in one short transaction only after the shadow generation has caught up.
* Duplicate rebuild requests while a job is active return that same job; the
  database has a unique constraint that permits only one active rebuild across
  both API worker processes.
* A failed or interrupted rebuild leaves the previous generation active. The
  committed pages and change log are durable, so a restart can rebuild again.

### OAI-PMH cataloguing harvest

Cataloguers fill citable `dc:title` / `dc:creator` metadata onto OCR pages.
The configured source (the bundled local demo OAI-PMH service in Compose) is
harvested with `ListRecords`, `metadataPrefix=oai_dc`, over a **closed UTC
second interval** (`from`/`until` are both inclusive). Each OAI `identifier`
is the archive page ID.

* `POST /api/harvests` starts a background job. A repeated trigger while one
  is active returns the **same job**; the two API processes can run only one
  harvest at a time thanks to a partial unique index.
* Every full-page catalog change and the resumption cursor commit in one
  short transaction. A stream break, bad XML or process exit never leaves a
  half-applied page; retriggering the same interval resumes the interrupted
  job from its saved cursor.
* Network waits happen **outside** database write transactions, so OCR
  corrections (`PUT /api/pages/...`) keep committing during a harvest.
* Deleted records only withdraw cataloguing (a `withdrawn` tombstone that
  keeps its deletion time); OCR text is never modified or removed.
* An expired/rejected resumption token re-harvests the original interval.
  Datestamp arbitration then makes the replay safe: older records are
  skipped, equal datestamp + equal content is a no-op, and equal datestamp +
  different content fails the whole job with a page-level error. Old records
  can never revive a tombstone.
* Read per-page cataloguing at `GET /api/pages/{page_id}/catalog` and job
  progress at `GET /api/harvests` / `GET /api/harvests/{job_id}`.

The `scenario` field drives the bundled demo source from Swagger:

| Scenario | What it demonstrates |
|---|---|
| (omit) | Normal paged harvest with one deleted record. |
| `broken_stream` | One truncated XML page; the job interrupts without a half page and a retrigger resumes to completion. |
| `expired_token` | The resumption token is rejected once; the original interval is re-harvested and deduplicated by datestamp. |
| `replay_old` | An older deleted record is replayed after a newer one; the tombstone/active state is not revived. |
| `conflict` | Same datestamp, different content; the job fails at page level instead of overwriting. |

### Two-minute Swagger demo

Use the **Try it out** controls below (or `curl`):

1. **Initial accented write (keep policy)** —
   `PUT /api/pages/ocr-001` with title `Crème brûlée à Paris` and body
   `Le café est près de la gare; le señor lit un naïve résumé.`
2. **Exact search** — `GET /api/search?q=café` returns the page.
   `GET /api/search?q=cafe` does not match under `keep`.
3. **Start a folding rebuild** — `POST /api/index/rebuild` with
   `{"accent_policy": "fold"}`. A second identical call returns the same
   `job_id` (HTTP 202).
4. **Correct and delete while it rebuilds** — send
   `PUT /api/pages/ocr-001` with replacement content, and
   `DELETE /api/pages/ocr-002`. Both commit immediately; the old index stays
   queryable.
5. **Watch status** — `GET /api/index/status` shows `active_policy`, the
   current job state (`queued`, `building`, `catching_up`, `activating`,
   `succeeded`), progress and both generations.
6. **After success** — `GET /api/search?q=cafe` now finds `café`-spelled text,
   deleted pages stay deleted, and returned titles/bodies are the corrected
   committed versions.

For a visible on-screen demonstration with a tiny dataset, run the API with
`REBUILD_BATCH_DELAY_MS=250` (or increase the page count). The delay is only a
demo aid and defaults to zero.

### Accent examples

| Document word | `keep` query | `fold` query |
|---|---|---|
| café | café | cafe or café |
| señor | señor | senor or señor |
| naïve | naïve | naive or naïve |
| résumé | résumé | resume or résumé |
"""


class PageUpsert(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "summary": "Initial accented OCR page (keep policy)",
                    "value": {
                        "title": "Crème brûlée à Paris",
                        "body": (
                            "Le café est près de la gare. Le señor lit un naïve "
                            "résumé sous la pluie."
                        ),
                    },
                },
                {
                    "summary": "Correction submitted while a rebuild is running",
                    "value": {
                        "title": "Crème brûlée — corrected OCR",
                        "body": (
                            "Le café fermé est près de la gare. Le señor relit "
                            "le résumé corrigé; l'élève est déjà parti."
                        ),
                    },
                },
            ]
        },
    )

    title: str = Field(default="", description="OCR page title (UTF-8, accents preserved)")
    body: str = Field(default="", description="OCR page body (UTF-8, accents preserved)")


class RebuildRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "summary": "Switch from keep to fold in the background",
                    "value": {"accent_policy": "fold"},
                },
                {
                    "summary": "Switch back to exact accents",
                    "value": {"accent_policy": "keep"},
                },
            ]
        },
    )

    accent_policy: AccentPolicy | None = Field(
        default=None,
        description="Target policy. Omit to choose the opposite of the active policy.",
    )
    # A friendly alias for clients that call the setting "policy".
    policy: AccentPolicy | None = Field(
        default=None,
        description="Alias of accent_policy.",
        exclude=True,
    )


class HarvestRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "summary": "Harvest a UTC-second closed interval",
                    "value": {
                        "from": "2026-01-01T00:00:00Z",
                        "until": "2026-12-31T23:59:59Z",
                    },
                },
                {
                    "summary": "Demo: truncated stream then resume on retrigger",
                    "value": {
                        "until": "2026-12-31T23:59:59Z",
                        "scenario": "broken_stream",
                    },
                },
                {
                    "summary": "Demo: expired resumption token forces interval re-harvest",
                    "value": {
                        "until": "2026-12-31T23:59:59Z",
                        "scenario": "expired_token",
                    },
                },
                {
                    "summary": "Demo: replay an old record after a deletion",
                    "value": {
                        "until": "2026-12-31T23:59:59Z",
                        "scenario": "replay_old",
                    },
                },
            ]
        },
    )

    from_datestamp: str | None = Field(
        default=None,
        alias="from",
        validation_alias="from",
        serialization_alias="from",
        description="Inclusive UTC OAI-PMH `from` bound, second granularity (e.g. 2026-01-01T00:00:00Z).",
    )
    until_datestamp: str = Field(
        ...,
        alias="until",
        validation_alias="until",
        serialization_alias="until",
        description="Inclusive UTC OAI-PMH `until` bound, second granularity.",
    )
    scenario: str | None = Field(
        default=None,
        description=(
            "Optional demo-source scenario passed as an OAI-ignored parameter: "
            "`broken_stream`, `expired_token` or `replay_old`."
        ),
    )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    logger.info("database ready at startup")
    yield
    cancel_local_rebuilds()
    cancel_local_harvests()


app = FastAPI(
    title="OCR Accent-Aware Archive Search",
    version="1.1.0",
    description=APP_DESCRIPTION,
    lifespan=lifespan,
    openapi_tags=[
        {"name": "pages", "description": "Transactional OCR page writes and deletes."},
        {"name": "catalog", "description": "Citable cataloguing (OAI-PMH dc:title/dc:creator) per page."},
        {"name": "harvest", "description": "OAI-PMH ListRecords harvesting with resumable jobs."},
        {"name": "search", "description": "Word search over the active FTS5 generation."},
        {
            "name": "index",
            "description": "Background generation rebuild and atomic accent-policy switch.",
        },
        {"name": "ops", "description": "Health and documentation helpers."},
    ],
)


@app.exception_handler(sqlite3.OperationalError)
async def sqlite_busy_handler(_request: Request, exc: sqlite3.OperationalError):
    logger.exception("sqlite operational error: %s", exc)
    return JSONResponse(
        status_code=503,
        content={
            "detail": "SQLite is temporarily busy; the write lock is not held for "
            "the duration of a rebuild. Retry the request.",
            "error": str(exc),
        },
    )


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/healthz", tags=["ops"], summary="Process and database health")
def health() -> dict:
    conn = connect(query_only=True)
    try:
        conn.execute("SELECT 1")
        generation = conn.execute(
            "SELECT value FROM settings WHERE key = 'active_policy'"
        ).fetchone()
        return {
            "status": "ok",
            "active_policy": generation[0] if generation else None,
            "pid": os.getpid(),
            "database": str(SETTINGS.db_path),
        }
    finally:
        conn.close()


@app.put(
    "/api/pages/{page_id}",
    tags=["pages"],
    summary="Insert or fully replace an OCR page by page ID",
    response_description="The page, current live index generation and change sequence.",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "accented_page": {
                            "summary": "Accented initial OCR text",
                            "value": {
                                "title": "Crème brûlée à Paris",
                                "body": "Le café est près de la gare; le señor lit un naïve résumé.",
                            },
                        },
                        "correction_during_rebuild": {
                            "summary": "Correction to submit while a rebuild runs",
                            "value": {
                                "title": "Crème brûlée — corrected",
                                "body": "Le café fermé est près de la gare; le señor relit le résumé corrigé.",
                            },
                        },
                    }
                }
            }
        }
    },
)
def put_page(
    page_id: Annotated[
        str,
        Path(
            min_length=1,
            max_length=512,
            description="Stable OCR page ID, e.g. `ocr-001`.",
            examples=["ocr-001", "scan-berlin-042"],
        ),
    ],
    payload: Annotated[
        PageUpsert,
        Body(
            description="Title and body are stored with their original accents. "
            "The active policy only affects the index/query representation."
        ),
    ],
) -> dict:
    try:
        return upsert_page(page_id, payload.title, payload.body)
    except Exception:
        logger.exception("page upsert failed for %s", page_id)
        raise


@app.delete(
    "/api/pages/{page_id}",
    tags=["pages"],
    summary="Delete an OCR page by ID (idempotent)",
    response_description="Deletion committed to the canonical store and live index.",
)
def remove_page(
    page_id: Annotated[
        str,
        Path(
            min_length=1,
            max_length=512,
            description="Page ID to delete, e.g. `ocr-002`.",
            examples=["ocr-002", "scan-berlin-051"],
        ),
    ],
) -> dict:
    try:
        return delete_page(page_id)
    except Exception:
        logger.exception("page delete failed for %s", page_id)
        raise


@app.get(
    "/api/pages/{page_id}",
    tags=["pages"],
    summary="Read a committed OCR page by ID",
)
def read_page(
    page_id: Annotated[str, Path(min_length=1, max_length=512, examples=["ocr-001"])],
) -> dict:
    page = get_page(page_id)
    if page is None:
        raise HTTPException(status_code=404, detail="page not found")
    return page


@app.get(
    "/api/search",
    tags=["search"],
    summary="Find pages whose title/body contains all queried words",
    response_description=(
        "Hits and titles/bodies are read in one SQLite snapshot from the same "
        "active generation and committed page state."
    ),
)
def search_pages(
    q: Annotated[
        str,
        Query(
            min_length=1,
            max_length=2048,
            description="One or more words. Matching is per word; all words must occur. "
            "Accents follow the active policy (`keep`, then `fold` after a switch).",
            openapi_examples={
                "exact_accent_keep": {
                    "summary": "Exact accent (keep policy)",
                    "value": "café",
                },
                "folded_spelling_after_switch": {
                    "summary": "Unaccented spelling (fold policy)",
                    "value": "cafe",
                },
                "two_words": {
                    "summary": "All words must match",
                    "value": "señor naïve",
                },
            },
        ),
    ],
    limit: Annotated[int, Query(ge=1, le=100, description="Maximum hits to return.")] = 20,
    offset: Annotated[int, Query(ge=0, description="Number of ordered hits to skip.")] = 0,
) -> dict:
    try:
        return search(q, limit=limit, offset=offset)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception:
        logger.exception("search failed for query %r", q)
        raise


def _trigger_rebuild(
    request: RebuildRequest | None,
    accent_policy: AccentPolicy | None,
    policy: AccentPolicy | None,
) -> dict:
    body_policy = None
    if request is not None:
        if (
            request.accent_policy is not None
            and request.policy is not None
            and request.accent_policy != request.policy
        ):
            raise HTTPException(
                status_code=422, detail="accent_policy and policy must not conflict"
            )
        body_policy = request.accent_policy or request.policy
    if accent_policy is not None and policy is not None and accent_policy != policy:
        raise HTTPException(status_code=422, detail="accent_policy and policy must not conflict")
    query_policy = accent_policy or policy
    if body_policy is not None and query_policy is not None and body_policy != query_policy:
        raise HTTPException(
            status_code=422,
            detail="body accent_policy and query accent_policy must not conflict",
        )
    target_policy = body_policy or query_policy

    try:
        result = claim_rebuild(target_policy)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    worker_token = result.pop("worker_token", None)
    if result["created"] and worker_token:
        job = result["job"]
        try:
            start_rebuild_thread(
                job_id=job["job_id"],
                generation_id=job["generation_id"],
                policy=job["to_policy"],
                claim_token=worker_token,
            )
        except Exception as exc:
            fail_job(
                job["job_id"],
                worker_token,
                state="failed",
                error=f"could not start rebuild worker: {exc}",
                generation_id=job["generation_id"],
            )
            raise HTTPException(
                status_code=503, detail="could not start background rebuild worker"
            ) from exc
    return result


@app.post(
    "/api/index/rebuild",
    tags=["index"],
    status_code=202,
    summary="Switch accent policy by rebuilding an index generation in the background",
    response_description=(
        "The newly queued job or the already-running job. Duplicate triggers while "
        "a rebuild is active return the same job ID."
    ),
    openapi_extra={
        "requestBody": {
            "required": False,
            "content": {
                "application/json": {
                    "examples": {
                        "fold": {
                            "summary": "Fold Latin accents after rebuild",
                            "value": {"accent_policy": "fold"},
                        },
                        "keep": {
                            "summary": "Restore exact accents after rebuild",
                            "value": {"accent_policy": "keep"},
                        },
                    }
                }
            },
        }
    },
)
def trigger_rebuild(
    request: Annotated[
        RebuildRequest | None,
        Body(description="Target accent policy. The body may be omitted; then the "
        "opposite of the active policy is used."),
    ] = None,
    accent_policy: Annotated[
        AccentPolicy | None,
        Query(description="Query-string alternative to the JSON body.", examples=["fold"]),
    ] = None,
    policy: Annotated[
        AccentPolicy | None,
        Query(description="Alias of accent_policy.", include_in_schema=False),
    ] = None,
) -> dict:
    return _trigger_rebuild(request, accent_policy, policy)


@app.post(
    "/api/index/switch",
    tags=["index"],
    status_code=202,
    include_in_schema=False,
)
def switch_alias(
    request: Annotated[RebuildRequest | None, Body()] = None,
    accent_policy: Annotated[AccentPolicy | None, Query()] = None,
    policy: Annotated[AccentPolicy | None, Query()] = None,
) -> dict:
    return _trigger_rebuild(request, accent_policy, policy)


@app.get(
    "/api/index/status",
    tags=["index"],
    summary="Inspect active generation, running rebuild and recent history",
    response_description=(
        "`active_policy` is the rule used by the very next query. "
        "`current_rebuild_job` is the single cross-process job, if any."
    ),
)
def index_status() -> dict:
    try:
        return get_status()
    except Exception:
        logger.exception("status query failed")
        raise


@app.get(
    "/api/index/jobs/{job_id}",
    tags=["index"],
    summary="Get one rebuild job by ID",
)
def index_job(
    job_id: Annotated[str, Path(min_length=1, description="Rebuild job UUID.")],
) -> dict:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="rebuild job not found")
    return job


def _validate_interval(from_datestamp: str | None, until_datestamp: str) -> tuple[str | None, str]:
    try:
        until_text = format_utc_second(parse_utc_second(until_datestamp))
        from_text = (
            format_utc_second(parse_utc_second(from_datestamp))
            if from_datestamp is not None
            else None
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if from_text is not None and from_text > until_text:
        raise HTTPException(
            status_code=422,
            detail="`from` must not be later than `until` (inclusive UTC-second interval)",
        )
    return from_text, until_text


@app.post(
    "/api/harvests",
    tags=["harvest"],
    status_code=202,
    summary="Start (or reuse/resume) an OAI-PMH ListRecords harvest for a UTC-second interval",
    response_description=(
        "The newly queued job, the resumed interrupted job, or the already-running job. "
        "Repeated triggers for the same source return the current task."
    ),
)
def trigger_harvest(payload: Annotated[HarvestRequest, Body()]) -> dict:
    from_datestamp, until_datestamp = _validate_interval(
        payload.from_datestamp, payload.until_datestamp
    )
    result = claim_harvest(
        source_url=SETTINGS.oai_base_url,
        metadata_prefix=SETTINGS.oai_metadata_prefix,
        from_datestamp=from_datestamp,
        until_datestamp=until_datestamp,
        scenario=payload.scenario,
    )

    worker_token = result.pop("worker_token", None)
    if worker_token:
        job = result["job"]
        try:
            start_harvest_thread(
                job_id=job["job_id"],
                claim_token=worker_token,
                source_url=job["source_url"],
                metadata_prefix=job["metadata_prefix"],
                from_datestamp=job["from_datestamp"],
                until_datestamp=job["until_datestamp"],
                scenario=job["scenario"],
            )
        except Exception as exc:  # pragma: no cover - thread start failure
            finish_harvest(
                job["job_id"],
                worker_token,
                state="failed",
                error=f"could not start harvest worker: {exc}",
            )
            raise HTTPException(status_code=503, detail="could not start harvest worker") from exc
    return result


@app.get(
    "/api/harvests",
    tags=["harvest"],
    summary="List harvest jobs and the currently active one, with live progress",
)
def harvests(
    limit: Annotated[int, Query(ge=1, le=100, description="Maximum jobs to return.")] = 20,
) -> dict:
    jobs = [harvest_progress(job) for job in list_harvest_jobs(limit=limit)]
    return {
        "source_url": SETTINGS.oai_base_url,
        "metadata_prefix": SETTINGS.oai_metadata_prefix,
        "active_harvest": harvest_progress(get_active_harvest()) if get_active_harvest() else None,
        "jobs": jobs,
        "server_time": utc_now(),
    }


@app.get(
    "/api/harvests/{job_id}",
    tags=["harvest"],
    summary="Get one harvest job, its progress counters and recent page errors",
)
def harvest_status(
    job_id: Annotated[str, Path(min_length=1, description="Harvest job UUID.")],
) -> dict:
    job = get_harvest_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="harvest job not found")
    return harvest_progress(job)


@app.get(
    "/api/pages/{page_id}/catalog",
    tags=["catalog"],
    summary="Read the citable cataloguing (titles, creators) harvested for one page",
    response_description=(
        "Stored dc:title/dc:creator values and record datestamp. A withdrawn page "
        "returns status `withdrawn` with empty title/creator lists and the deletion time."
    ),
)
def read_catalog(
    page_id: Annotated[
        str,
        Path(
            min_length=1,
            max_length=512,
            description="OAI identifier / OCR page ID, e.g. `ocr-001`.",
            examples=["ocr-001"],
        ),
    ],
) -> dict:
    catalog = get_catalog(page_id)
    if catalog is None:
        raise HTTPException(
            status_code=404,
            detail="no cataloguing harvested for this page yet",
        )
    return catalog
