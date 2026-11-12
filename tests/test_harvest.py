"""Integration tests for OAI-PMH harvesting, catalog writes and job coordination."""

from __future__ import annotations

import socket
import threading
import time
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import SETTINGS
from demo_oai.server import OaiRequestHandler

INTERVAL = {"until": "2026-12-31T23:59:59Z"}


@pytest.fixture
def demo_server():
    # Fresh server per test so one-shot demo faults always start un-fired.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    server = ThreadingHTTPServer(("127.0.0.1", port), OaiRequestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}/oai"
    server.shutdown()
    server.server_close()
    thread.join(timeout=3)


@pytest.fixture
def client(tmp_path: Path, demo_server: str):
    object.__setattr__(SETTINGS, "db_path", tmp_path / "archive.db")
    object.__setattr__(SETTINGS, "oai_base_url", demo_server)
    object.__setattr__(SETTINGS, "oai_metadata_prefix", "oai_dc")
    object.__setattr__(SETTINGS, "harvest_lease_seconds", 5.0)
    object.__setattr__(SETTINGS, "oai_timeout_seconds", 5.0)
    from app.main import app
    from app.rebuild import cancel_local_rebuilds

    with TestClient(app) as test_client:
        yield test_client
    cancel_local_rebuilds()
    from app.harvest import cancel_local_harvests

    cancel_local_harvests()


def _start_harvest(client: TestClient, payload: dict | None = None) -> dict:
    body = dict(INTERVAL)
    if payload:
        body.update(payload)
    response = client.post("/api/harvests", json=body)
    assert response.status_code == 202, response.text
    return response.json()


def _wait_terminal(client: TestClient, job_id: str, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = client.get(f"/api/harvests/{job_id}").json()
        if job["state"] in {"succeeded", "failed", "interrupted"}:
            return job
        time.sleep(0.02)
    raise AssertionError(f"harvest {job_id} did not finish within {timeout}s")


def test_full_harvest_stores_all_titles_creators_and_tombstone(client: TestClient):
    started = _start_harvest(client)
    assert started["created"] is True
    job = _wait_terminal(client, started["job"]["job_id"])
    assert job["state"] == "succeeded", job
    assert job["requests_made"] == 3  # five records, page size 2
    assert job["records_applied"] == 4
    assert job["records_withdrawn"] == 1

    page = client.get("/api/pages/ocr-001/catalog")
    assert page.status_code == 200
    catalog = page.json()
    assert catalog["status"] == "active"
    assert catalog["titles"] == ["Crème brûlée à Paris", "Parisian Desserts, vol. 1"]
    assert catalog["creators"] == ["Amélie Durand", "Henri Laurent"]
    assert catalog["record_datestamp"] == "2026-01-05T09:00:00Z"
    assert catalog["source"].endswith("/oai")

    withdrawn = client.get("/api/pages/ocr-003/catalog").json()
    assert withdrawn["status"] == "withdrawn"
    assert withdrawn["titles"] == []
    assert withdrawn["creators"] == []
    assert withdrawn["record_datestamp"] == "2026-01-07T11:15:00Z"

    assert client.get("/api/pages/never-seen/catalog").status_code == 404


def test_deleted_record_only_withdraws_cataloguing_and_keeps_ocr_text(
    client: TestClient,
):
    # OCR text exists independently of cataloguing and must survive harvesting.
    saved = client.put(
        "/api/pages/ocr-003",
        json={"title": "Manually OCR'd page three", "body": "valuable OCR text"},
    )
    assert saved.status_code == 200
    revision = saved.json()["revision"]

    job = _wait_terminal(client, _start_harvest(client)["job"]["job_id"])
    assert job["state"] == "succeeded"

    withdrawn = client.get("/api/pages/ocr-003/catalog").json()
    assert withdrawn["status"] == "withdrawn"

    page = client.get("/api/pages/ocr-003").json()
    assert page["body"] == "valuable OCR text"
    assert page["revision"] == revision


def test_duplicate_trigger_returns_the_same_running_job(client: TestClient):
    first = _start_harvest(client)
    second = _start_harvest(client)
    assert second["created"] is False
    assert second["reused_existing"] is True
    assert second["job"]["job_id"] == first["job"]["job_id"]

    job = _wait_terminal(client, first["job"]["job_id"])
    assert job["state"] == "succeeded"

    # Once finished, retriggering the identical interval replays everything;
    # equal-datestamp/equal-content records are all skipped.
    replay = _start_harvest(client)
    assert replay["created"] is True
    replayed = _wait_terminal(client, replay["job"]["job_id"])
    assert replayed["state"] == "succeeded"
    assert replayed["records_applied"] == 0
    assert replayed["records_skipped"] == 5

    listing = client.get("/api/harvests").json()
    assert listing["active_harvest"] is None
    assert len(listing["jobs"]) == 2


def test_broken_stream_leaves_no_partial_page_and_retrigger_resumes(
    client: TestClient,
):
    started = _start_harvest(client, {"scenario": "broken_stream"})
    job_id = started["job"]["job_id"]
    job = _wait_terminal(client, job_id)
    assert job["state"] == "interrupted", job
    assert "ParseError" in (job["error"] or "")

    # Page 1 was fully committed before the broken second page arrived.
    first = client.get("/api/pages/ocr-001/catalog")
    assert first.status_code == 200
    # The page carried by the truncated response must not exist half-applied.
    assert client.get("/api/pages/ocr-003/catalog").status_code == 404

    # Retrigger the same interval: the interrupted job is claimed and resumed.
    resumed = _start_harvest(client, {"scenario": "broken_stream"})
    assert resumed["resumed"] is True
    assert resumed["job"]["job_id"] == job_id
    finished = _wait_terminal(client, job_id)
    assert finished["state"] == "succeeded", finished
    assert client.get("/api/pages/ocr-003/catalog").json()["status"] == "withdrawn"
    assert client.get("/api/pages/ocr-005/catalog").json()["status"] == "active"


def test_expired_resumption_token_reharvests_interval_and_deduplicates(
    client: TestClient,
):
    started = _start_harvest(client, {"scenario": "expired_token"})
    job = _wait_terminal(client, started["job"]["job_id"])
    assert job["state"] == "succeeded", job
    assert job["restarts"] == 1
    # One successful page, one rejected-token 400, then a full 3-page replay.
    assert job["requests_made"] == 5
    # The token fault arrived after page 1 committed, so the replay skips only
    # those two equal-datestamp/equal-content records; pages 2-3 apply fresh.
    assert job["records_seen"] == 5
    assert job["records_applied"] == 4
    assert job["records_skipped"] == 2
    assert job["records_withdrawn"] == 1
    # Every streamed record (two sweeps x five) is accounted for exactly once.
    assert job["records_processed_total"] == 7

    catalog = client.get("/api/pages/ocr-001/catalog").json()
    assert catalog["titles"] == ["Crème brûlée à Paris", "Parisian Desserts, vol. 1"]


def test_old_deleted_record_cannot_revive_or_overwrite(client: TestClient):
    job = _wait_terminal(
        client, _start_harvest(client, {"scenario": "replay_old"})["job"]["job_id"]
    )
    assert job["state"] == "succeeded", job
    # The replay-old extra row has an older datestamp and is skipped.
    assert job["records_skipped"] == 1

    page = client.get("/api/pages/ocr-004/catalog").json()
    assert page["status"] == "active"
    assert page["record_datestamp"] == "2026-01-08T14:45:00Z"
    assert page["titles"] == ["Señor y naïve: notas de lectura"]


def test_same_datestamp_different_content_fails_whole_job_with_page_error(
    client: TestClient,
):
    started = _start_harvest(client, {"scenario": "conflict"})
    job = _wait_terminal(client, started["job"]["job_id"])
    assert job["state"] == "failed", job
    assert "ocr-002" in (job["error"] or "")
    assert job["recent_errors"], job
    error = job["recent_errors"][0]
    assert error["page_id"] == "ocr-002"
    assert error["kind"] == "datestamp_conflict"

    # The pre-conflict pages remain committed; nothing partial after it.
    assert client.get("/api/pages/ocr-001/catalog").status_code == 200


def test_harvest_does_not_block_ocr_corrections(client: TestClient):
    # The demo source responds quickly; submit OCR writes around a harvest and
    # prove they commit independently of cataloguing.
    started = _start_harvest(client)
    for attempt in range(20):
        correction = client.put(
            "/api/pages/ocr-001",
            json={"title": "Live correction", "body": f"fresh OCR body {attempt}"},
        )
        assert correction.status_code == 200
    job = _wait_terminal(client, started["job"]["job_id"])
    assert job["state"] == "succeeded"

    page = client.get("/api/pages/ocr-001").json()
    assert page["body"] == "fresh OCR body 19"
    catalog = client.get("/api/pages/ocr-001/catalog").json()
    # Cataloguing and OCR text are separate stores, both still readable.
    assert catalog["titles"][0] == "Crème brûlée à Paris"


def test_interval_validation_and_progress_endpoint(client: TestClient):
    bad = client.post(
        "/api/harvests",
        json={"from": "2026-02-01T00:00:00Z", "until": "2026-01-01T00:00:00Z"},
    )
    assert bad.status_code == 422

    malformed = client.post("/api/harvests", json={"until": "not-a-date"})
    assert malformed.status_code == 422

    missing = client.get("/api/harvests/does-not-exist")
    assert missing.status_code == 404


def test_orphaned_harvest_is_interrupted_on_restart_and_resumable(
    tmp_path: Path, demo_server: str,
):
    object.__setattr__(SETTINGS, "db_path", tmp_path / "restart.db")
    object.__setattr__(SETTINGS, "oai_base_url", demo_server)
    from app.db import connect, init_db
    from app.harvest_store import claim_harvest, get_harvest_job

    init_db()
    claimed = claim_harvest(
        source_url=demo_server,
        metadata_prefix="oai_dc",
        from_datestamp=None,
        until_datestamp="2026-12-31T23:59:59Z",
        scenario="broken_stream",
    )
    job_id = claimed["job"]["job_id"]

    # Simulate a container restart: the owning process is a foreign host.
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE harvest_jobs SET owner_host = 'previous-container', "
            "owner_pid = 999999, state = 'running' WHERE id = ?",
            (job_id,),
        )
        conn.commit()
    finally:
        conn.close()

    init_db()
    job = get_harvest_job(job_id)
    assert job["state"] == "interrupted"

    # A new trigger resumes the same job rather than creating a duplicate.
    retried = claim_harvest(
        source_url=demo_server,
        metadata_prefix="oai_dc",
        from_datestamp=None,
        until_datestamp="2026-12-31T23:59:59Z",
        scenario="broken_stream",
    )
    assert retried["resumed"] is True
    assert retried["job"]["job_id"] == job_id


def test_empty_list_records_completes_without_catalog_rows(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    from app import harvest as harvest_module
    from app.oai import ListRecordsResponse

    # harvest.py binds list_records via "from .oai import", so patch there.
    monkeypatch.setattr(
        harvest_module,
        "list_records",
        lambda *a, **k: ListRecordsResponse(records=(), resumption_token=None),
    )
    started = _start_harvest(client)
    job = _wait_terminal(client, started["job"]["job_id"])
    assert job["state"] == "succeeded"
    assert job["requests_made"] == 1
    assert job["resumption_token"] is None
    assert job["records_seen"] == 0
    assert client.get("/api/pages/ocr-001/catalog").status_code == 404


def test_protocol_error_fails_job_and_is_surfaced(client: TestClient, monkeypatch):
    from app import harvest as harvest_module
    from app.oai import HarvestProtocolError

    def boom(*a, **k):
        raise HarvestProtocolError("cannotDisseminateFormat", "unsupported prefix")

    monkeypatch.setattr(harvest_module, "list_records", boom)
    started = _start_harvest(client)
    job = _wait_terminal(client, started["job"]["job_id"])
    assert job["state"] == "failed"
    assert "cannotDisseminateFormat" in (job["error"] or "")
