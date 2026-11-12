# OCR Accent-Aware Archive Search

FastAPI + SQLite FTS5 service for an OCR archive whose searchable accent
policy can be changed online. Page corrections and deletes continue to commit
while a new index is being built; the old index serves searches until the new
generation has caught up and is switched atomically. A second feature set
harvests citable cataloguing (`dc:title` / `dc:creator`) from a configured
OAI-PMH 2.0 source into transactional, restartable harvest jobs.

## Start with Docker Compose

```bash
docker compose up --build -d
```

Open **<http://localhost:8000/docs>**. Two containers start:

* `api` — exactly two uvicorn worker processes sharing one WAL-mode SQLite
  file on the named volume `archive-data`;
* `demo-oai` — a dependency-free local OAI-PMH 2.0 demo source on port 8010
  (`Identify` and paged `ListRecords` with `metadataPrefix=oai_dc`), wired to
  the API through the fixed `OAI_BASE_URL=http://demo-oai:8010/oai`.

```bash
docker compose ps
docker compose exec api ps -ef | grep '[u]vicorn'
```

The `api` container is the single writer-capable deployment; SQLite is not
shared between separate containers. The two processes are uvicorn workers in
this one container.

To make a rebuild visibly long enough to submit edits by hand during it:

```bash
REBUILD_BATCH_DELAY_MS=250 docker compose up --build -d
```

The delay is a demo-only aid and is `0` by default. Reset demo data with:

```bash
docker compose down -v
```

The demo source's host port defaults to 8010 (`DEMO_OAI_PORT`); the API
defaults to 8000 (`API_PORT`).

## Swagger demo

Every step is available with **Try it out** on `/docs`. The OpenAPI description
also contains the full demo narrative and accented request examples.

### 1. Write two accented OCR pages under the initial `keep` policy

```bash
BASE=http://localhost:8000

curl -sS -X PUT "$BASE/api/pages/ocr-001" \
  -H 'Content-Type: application/json' \
  -d '{
    "title": "Crème brûlée à Paris",
    "body": "Le café est près de la gare; le señor lit un naïve résumé."
  }'

curl -sS -X PUT "$BASE/api/pages/ocr-002" \
  -H 'Content-Type: application/json' \
  -d '{
    "title": "A plain cafe",
    "body": "This page uses cafe and naive without accents."
  }'
```

### 2. Search with the initial exact-accent rule

```bash
curl --get "$BASE/api/search" --data-urlencode 'q=café'
# ocr-001 is returned.

curl --get "$BASE/api/search" --data-urlencode 'q=cafe'
# Only ocr-002 matches. The accented café is not folded under keep.
```

Word queries are literal words. Multiple words are ANDed; FTS query operators
from the request are never interpreted.

### 3. Trigger the background switch to `fold`

```bash
curl -sS -X POST "$BASE/api/index/rebuild" \
  -H 'Content-Type: application/json' \
  -d '{"accent_policy": "fold"}'
```

The response is HTTP `202` and contains a `job.job_id`. A duplicate trigger
while the job is active returns the **same** job ID with
`"reused_existing_rebuild": true`.

### 4. Correct and delete during the rebuild

Run these while `GET /api/index/status` reports `building` or `catching_up`:

```bash
# Correction: replaces the page and becomes visible in both the old live index
# and, after catch-up, the new generation.
curl -sS -X PUT "$BASE/api/pages/ocr-001" \
  -H 'Content-Type: application/json' \
  -d '{
    "title": "Crème brûlée — corrected OCR",
    "body": "Le café fermé est près de la gare; le señor relit le résumé corrigé."
  }'

# Deletion must remain deleted; the old snapshot scan must not revive it.
curl -sS -X DELETE "$BASE/api/pages/ocr-002"
```

These writes commit immediately. The rebuild scan does not hold a lock for its
duration, so editing is not blocked by it.

### 5. Watch the live status and old index

```bash
curl -sS "$BASE/api/index/status"
curl --get "$BASE/api/search" --data-urlencode 'q=cafe'
```

While the job is running, `active_policy` is still `keep`,
`rebuild_in_progress` is `true`, and `current_rebuild_job` shows progress:
`queued → building → catching_up → activating → succeeded`.

### 6. Query after the switch

Once `current_rebuild_job` is `null` and `active_policy` is `fold`:

```bash
curl --get "$BASE/api/search" --data-urlencode 'q=cafe'
# Returns ocr-001 with the corrected body: café matches cafe.

curl --get "$BASE/api/search" --data-urlencode 'q=resume'
# Folded résumé matches.
```

Deleted `ocr-002` remains absent, and returned titles/bodies are the corrected
committed versions from the same SQLite snapshot as the hit.

## OAI-PMH cataloguing harvest

Cataloguers attach citable titles and authors to OCR pages. The API harvests
the fixed configured source with OAI-PMH `ListRecords`,
`metadataPrefix=oai_dc`, over a **closed UTC-second interval**: both `from`
and `until` are inclusive and normalized to whole UTC seconds. Each OAI
`identifier` is treated as the archive page ID. All repeated `dc:title` and
`dc:creator` elements are stored, in document order.

### Start a harvest and read progress

```bash
BASE=http://localhost:8000

curl -sS -X POST "$BASE/api/harvests" \
  -H 'Content-Type: application/json' \
  -d '{"from":"2026-01-01T00:00:00Z","until":"2026-12-31T23:59:59Z"}'
# HTTP 202 with a job_id. A second identical trigger while it runs returns
# the SAME job ("reused_existing": true).

curl -sS "$BASE/api/harvests"                       # active job + history
curl -sS "$BASE/api/harvests/<job_id>"             # counters and page errors
curl -sS "$BASE/api/pages/ocr-001/catalog"         # per-page cataloguing
```

A catalog response looks like:

```json
{
  "page_id": "ocr-001",
  "status": "active",
  "titles": ["Crème brûlée à Paris", "Parisian Desserts, vol. 1"],
  "creators": ["Amélie Durand", "Henri Laurent"],
  "source": "http://demo-oai:8010/oai",
  "record_datestamp": "2026-01-05T09:00:00Z",
  "first_seen_at": "…",
  "updated_at": "…",
  "harvest_job_id": "…"
}
```

### Transactional and restartable

* Each record's full-page catalog change is committed in one short
  `BEGIN IMMEDIATE` transaction **together with the resumption cursor**, so a
  network drop, truncated/bad XML response or process exit can never leave a
  half-applied page. On restart, dead owners are marked `interrupted` at
  startup, and retriggering the identical interval resumes the same job from
  its persisted `resumptionToken`.
* The blocking HTTP call happens with **no database transaction open**, so
  the SQLite write lock is never held across network waits and OCR
  corrections keep committing during a harvest.
* The two uvicorn workers share one active-harvest slot enforced by a partial
  unique index plus a claim token and heartbeat lease; a duplicate trigger
  always returns the current task.

### Deleted records and datestamp arbitration

A `status="deleted"` OAI record only withdraws cataloguing: it creates or
refreshes a `withdrawn` tombstone that keeps the deletion time. The canonical
OCR `pages` table (title/body text) is never touched by harvesting.

When an expired/rejected token forces a full re-harvest of the original
interval (the worker does this automatically on `badResumptionToken`/
`expired`), every replayed record is arbitrated by its UTC-second datestamp:

| Incoming vs stored | Result |
|---|---|
| older datestamp | skipped — nothing changes, tombstones never revive |
| equal datestamp, equal titles/creators | skipped (idempotent no-op) |
| equal datestamp, different content | whole job `failed`, page error recorded |
| newer datestamp | applied (active) or withdrawn (deleted) |

### Demo scenarios on `/docs`

The bundled demo source reacts to the optional `scenario` field (sent as an
OAI-ignored `demo_scenario` parameter on the first request and embedded in
the opaque resumption token afterwards). Each one-shot fault fires exactly
once per demo-source process, so retries always succeed.

| `scenario` | Demonstration |
|---|---|
| omitted | Normal paged harvest; five records, one deleted tombstone. |
| `broken_stream` | One truncated, non-well-formed XML page. The job becomes `interrupted` with no partial page; retrigger to resume and finish. |
| `expired_token` | First resumption token is rejected with `badResumptionToken`; the worker restarts the interval and deduplicates by datestamp (`restarts` becomes 1). |
| `replay_old` | An older deleted `ocr-004` is streamed after its newer active version; it is skipped and the active record survives. |
| `conflict` | Same identifier + same datestamp but different content; the job fails with a `datestamp_conflict` page error instead of overwriting. |

```bash
# Bad page, then resume
curl -sS -X POST "$BASE/api/harvests" -H 'Content-Type: application/json' \
  -d '{"until":"2026-12-31T23:59:59Z","scenario":"broken_stream"}'
# inspect the interrupted job, then POST the same body again -> resumed, succeeds
```

## Accent examples

| Document word | `keep` query | `fold` query |
|---|---|---|
| café | café | cafe or café |
| señor | señor | senor or señor |
| naïve | naïve | naive or naïve |
| résumé | résumé | resume or résumé |
| Crème brûlée | crème / brûlée | creme / brulee |
| İstanbul | İstanbul | Istanbul |

`keep` normalises composed/decomposed spellings to NFC but never removes a
diacritic. `fold` removes combining marks attached to Latin base letters only;
non-Latin combining marks are preserved.

## API summary

| Method | Path | Purpose |
|---|---|---|
| `PUT` | `/api/pages/{page_id}` | Insert/replace an OCR title and body. |
| `DELETE` | `/api/pages/{page_id}` | Idempotent delete. |
| `GET` | `/api/pages/{page_id}` | Read a committed page. |
| `GET` | `/api/pages/{page_id}/catalog` | Read harvested citable titles/authors (incl. withdrawn tombstones). |
| `POST` | `/api/harvests` | Start/reuse/resume an OAI-PMH ListRecords harvest for a UTC-second interval. |
| `GET` | `/api/harvests` | Active harvest and job history with progress counters. |
| `GET` | `/api/harvests/{job_id}` | One harvest job, counters and recent page-level errors. |
| `GET` | `/api/search?q=word` | Word search over the active FTS generation. |
| `POST` | `/api/index/rebuild` | Queue/switch accent policy through a background rebuild. |
| `GET` | `/api/index/status` | Active policy/generation, current job, progress, history. |
| `GET` | `/api/index/jobs/{job_id}` | One rebuild job. |
| `GET` | `/healthz` | Health and active policy. |

`POST /api/index/rebuild` also accepts `?accent_policy=fold` for convenience.
If the body/parameter is omitted, the opposite of the active policy is used.
The alias `POST /api/index/switch` is available but omitted from the Swagger
schema.

## Correctness model

* **Durable source of truth** — `pages` stores the latest committed title/body
  and `changes` is an append-only sequence log containing the full content of
  every write/delete.
* **One active generation** — each generation owns a separate FTS5 table.
  `settings.active_generation_id` and `settings.active_policy` are switched in
  the same short transaction that marks the new generation active.
* **Writes during rebuild** — a write transaction updates `pages`, appends to
  `changes`, and updates the currently active FTS table. It never writes to a
  generation under construction. Therefore no scan-length write lock is needed,
  and a failed worker cannot corrupt the serving index.
* **Snapshot baseline** — the rebuild reads its baseline in a pinned WAL read
  transaction. Writers continue to commit in WAL while that snapshot is open.
* **Catch-up** — the worker applies every change-log row after the snapshot,
  in sequence, to its private shadow FTS table. It does this in small write
  transactions, with heartbeats.
* **Atomic swap** — the final short `BEGIN IMMEDIATE` transaction applies the
  last residual changes, verifies shadow/page document counts, updates the
  pointer/policy, and marks the job succeeded. Only then does a new request see
  the new generation and policy.
* **Snapshot-consistent reads** — a search begins one SQLite read transaction,
  reads the active pointer/policy, the FTS hits and the canonical bodies in that
  same transaction. Hits and returned text therefore come from one committed
  state, even if a swap commits concurrently.
* **One rebuild across processes** — `rebuild_jobs` has a partial unique index
  on active states. Claiming is done under `BEGIN IMMEDIATE`, so two API
  processes cannot start two jobs; the loser returns the current job.
* **Failure/interruption** — the previous active generation is never changed
  until final commit. A failed job marks only its shadow generation failed. A
  new process marks a dead owner's job interrupted at startup (dead PID or a
  different container host), so the rebuild can be retriggered immediately.
  Canonical pages and the change log survive in SQLite.
* **Fencing** — the final activation re-checks the job's claim token and
  active state under the write lock, so a stale worker that resumes cannot
  switch an obsolete generation after a newer job has been claimed.

FTS rowids are taken from the canonical page rowid and every log entry carries
that rowid, making updates/deletes O(index lookup) rather than rescanning the
FTS table by page ID.

Retired FTS tables are deliberately retained rather than dropped in the swap
transaction. This avoids schema-lock surprises for an already-running query;
generation history is visible in `/api/index/status`.

### Harvest correctness model

* **Separate catalog store** — harvested titles/authors live in `catalog`,
  never in the OCR `pages`/`changes` tables. A deleted OAI record writes a
  `withdrawn` tombstone (preserving the deletion time) and cannot alter OCR
  text. OCR corrections therefore remain committable at any time.
* **One transaction per full-page change** — the catalog upsert/tombstone,
  counters and the next resumption cursor commit together under
  `BEGIN IMMEDIATE`. A crash, stream break or XML parse error rolls back the
  whole page; the durable cursor means no committed page is fetched twice.
* **Network outside the lock** — ListRecords HTTP calls run with no open DB
  transaction, so the writer lock is held only for the short page commits.
* **Datestamp arbitration on replay** — decisions use normalized UTC-second
  datestamps: older records skip, equal-time/equal-content no-ops skip,
  equal-time/different-content fails the job (with a row in
  `harvest_errors`), newer records apply. Tombstones therefore can never be
  revived by an old record.
* **Token expiry re-harvest** — a `badResumptionToken`/`expired` protocol
  response clears the cursor and restarts the *original* closed interval,
  incrementing `restarts`; the arbitration rules make the replay idempotent.
* **One harvest across processes** — `harvest_jobs` has a partial unique
  index on active states and a claim token fences every worker. Dead owners
  (foreign host, dead PID or reused PID) are marked `interrupted` at startup;
  retriggering the same interval resumes that job rather than duplicating it.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `ARCHIVE_DB_PATH` | `./data/archive.db` locally, `/data/archive.db` in Docker | SQLite path. |
| `OAI_BASE_URL` | `http://127.0.0.1:8010/oai` locally, `http://demo-oai:8010/oai` in Docker | Fixed OAI-PMH source endpoint. |
| `OAI_METADATA_PREFIX` | `oai_dc` | Metadata prefix for ListRecords. |
| `OAI_TIMEOUT_SECONDS` | `20` | Per-request network timeout (waited outside DB transactions). |
| `HARVEST_LEASE_SECONDS` | `30` (20 in Compose) | Heartbeat lease before a harvest may be reclaimed. |
| `HARVEST_PAGE_SIZE` | `100` | Hint for harvest batching/bookkeeping. |
| `REBUILD_LEASE_SECONDS` | `20` | Heartbeat lease before a trigger may reclaim a job. |
| `REBUILD_BATCH_SIZE` | `500` | Scan/catch-up batch size. |
| `REBUILD_BATCH_DELAY_MS` | `0` | Demo-only pacing per batch. |
| `DB_BUSY_TIMEOUT_SECONDS` | `30` | SQLite writer wait. |

## Local development and tests

```bash
python3 -m pip install -r requirements.txt -r requirements-dev.txt

# Terminal 1: local OAI-PMH demo source (standard library only, port 8010)
python3 demo_oai/server.py

# Terminal 2: two-worker API
ARCHIVE_DB_PATH=/tmp/archive-dev.db \
  uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 2
```

```bash
pytest -q
```

The tests exercise exact vs folded search, a rebuild that finishes with a
correction and a deletion submitted during the scan, duplicate job idempotency,
and restart/stale-job recovery. They use temporary SQLite files. The harvest
tests boot the demo source on an ephemeral port and cover normal paging,
tombstone-vs-OCR isolation, duplicate-trigger reuse, broken-stream resume,
expired-token re-harvest, old-record replay, same-datestamp conflicts,
concurrent OCR writes and orphaned-job recovery.
