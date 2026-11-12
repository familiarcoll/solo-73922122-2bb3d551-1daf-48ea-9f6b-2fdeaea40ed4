"""Local OAI-PMH 2.0 demo source for the archive harvester.

A self-contained, dependency-free ``http.server`` application that implements
just enough of OAI-PMH (``Identify`` and ``ListRecords`` with
``metadataPrefix=oai_dc``) to exercise every harvester guarantee from
``/docs``:

* normal paged harvesting, including a ``deleted`` tombstone and repeated
  ``dc:title`` / ``dc:creator`` elements;
* ``broken_stream``  - one truncated, non-well-formed XML response, then a
  clean replay so a retriggered job resumes and completes;
* ``expired_token``  - the first presented resumption token is rejected with
  ``badResumptionToken`` once, forcing a full re-harvest of the interval;
* ``replay_old``     - an older, previously deleted record is replayed after
  its newer replacement, proving old records cannot revive a tombstone;
* ``conflict``       - a same-datestamp/different-content contradiction that
  must fail the whole job at the page level instead of silently overwriting.

Scenario control travels as the non-standard ``demo_scenario`` query
parameter on the *first* request and is embedded inside the opaque
resumption token afterwards, exactly like a real server-side cursor.
"""

from __future__ import annotations

import base64
import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from xml.sax.saxutils import escape

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
DC_NS = "http://purl.org/dc/elements/1.1/"

# Fixed UTC-second datestamps keep every demo deterministic across restarts.
_BASE_RECORDS = [
    {
        "id": "ocr-001",
        "datestamp": "2026-01-05T09:00:00Z",
        "titles": ["Crème brûlée à Paris", "Parisian Desserts, vol. 1"],
        "creators": ["Amélie Durand", "Henri Laurent"],
    },
    {
        "id": "ocr-002",
        "datestamp": "2026-01-06T10:30:00Z",
        "titles": ["A Plain Cafe Manifesto"],
        "creators": ["John Smith"],
    },
    {
        "id": "ocr-003",
        # Deleted tombstone: harvesting must withdraw cataloguing only and
        # leave any OCR page text untouched.
        "id": "ocr-003",
        "datestamp": "2026-01-07T11:15:00Z",
        "deleted": True,
    },
    {
        "id": "ocr-004",
        "datestamp": "2026-01-08T14:45:00Z",
        "titles": ["Señor y naïve: notas de lectura"],
        "creators": ["Lucía Fernández", "Carlos Ruiz"],
    },
    {
        "id": "ocr-005",
        "datestamp": "2026-01-09T08:05:00Z",
        "titles": ["Archive Rebuild Procedures"],
        "creators": ["Ivana Nováková"],
    },
]

# replay_old: an old deleted version of ocr-004 is streamed *after* the newer
# active one.  The datestamp is older, so the tombstone must not come back.
_REPLAY_OLD_EXTRA = {
    "id": "ocr-004",
    "datestamp": "2025-12-31T23:59:00Z",
    "deleted": True,
}

# conflict: identical identifier and datestamp as ocr-002 but different
# dc:title/dc:creator content - a deterministic whole-job failure.
_CONFLICT_EXTRA = {
    "id": "ocr-002",
    "datestamp": "2026-01-06T10:30:00Z",
    "titles": ["A Completely Different Title"],
    "creators": ["Another Author"],
}

PAGE_SIZE = 2

# Each one-shot fault fires exactly once per server process.  A resumed
# request therefore always gets a well-behaved response.
_fault_locks = threading.Lock()
_fired_faults: set[str] = set()


def _fault_already_fired(name: str) -> bool:
    with _fault_locks:
        if name in _fired_faults:
            return True
        _fired_faults.add(name)
        return False


def encode_token(cursor: int, scenario: str | None) -> str:
    payload = json.dumps({"c": cursor, "s": scenario or ""}, separators=(",", ":")).encode()
    return "demo-" + base64.urlsafe_b64encode(payload).decode("ascii")


def decode_token(token: str) -> tuple[int, str | None]:
    if not token.startswith("demo-"):
        raise ValueError("not a demo token")
    raw = base64.urlsafe_b64decode(token[len("demo-"):].encode("ascii"))
    payload = json.loads(raw.decode("ascii"))
    cursor = int(payload["c"])
    scenario = payload.get("s") or None
    return cursor, scenario


def _dataset_for(scenario: str | None) -> list[dict]:
    records = [dict(item) for item in _BASE_RECORDS]
    if scenario == "replay_old":
        records.append(dict(_REPLAY_OLD_EXTRA))
    elif scenario == "conflict":
        records.append(dict(_CONFLICT_EXTRA))
    return records


def _envelope(verb: str, body: str) -> bytes:
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<OAI-PMH xmlns="{OAI_NS}" xmlns:oai_dc="{OAI_DC_NS}" '
        f'xmlns:dc="{DC_NS}">\n'
        f"  <responseDate>2026-01-10T00:00:00Z</responseDate>\n"
        f"  <request>http://demo-oai:8010/oai?verb={verb}</request>\n"
        f"  {body}\n"
        "</OAI-PMH>\n"
    )
    return xml.encode("utf-8")


def _error_envelope(code: str, message: str) -> bytes:
    body = f'<error code="{escape(code)}">{escape(message)}</error>'
    return _envelope("ListRecords", body)


def _record_xml(record: dict) -> str:
    # The OAI identifier is the archive page ID itself, as required by the
    # harvesting contract (identifier -> page ID).
    identifier = escape(record["id"])
    datestamp = escape(record["datestamp"])
    if record.get("deleted"):
        return (
            "    <record>\n"
            '      <header status="deleted">\n'
            f"        <identifier>{identifier}</identifier>\n"
            f"        <datestamp>{datestamp}</datestamp>\n"
            "      </header>\n"
            "    </record>"
        )
    titles = "".join(
        f"\n          <dc:title>{escape(text)}</dc:title>" for text in record["titles"]
    )
    creators = "".join(
        f"\n          <dc:creator>{escape(text)}</dc:creator>"
        for text in record["creators"]
    )
    return (
        "    <record>\n"
        "      <header>\n"
        f"        <identifier>{identifier}</identifier>\n"
        f"        <datestamp>{datestamp}</datestamp>\n"
        "      </header>\n"
        "      <metadata>\n"
        '        <oai_dc:dc>\n'
        f"{titles}{creators}\n"
        "        </oai_dc:dc>\n"
        "      </metadata>\n"
        "    </record>"
    )


def _list_records_xml(scenario: str | None, cursor: int, complete_size: int) -> str:
    records = _dataset_for(scenario)
    page = records[cursor : cursor + PAGE_SIZE]
    next_cursor = cursor + len(page)
    parts = ["  <ListRecords>"]
    parts.extend(_record_xml(record) for record in page)
    if next_cursor < len(records):
        token = encode_token(next_cursor, scenario)
        parts.append(
            f'    <resumptionToken completeListSize="{len(records)}" '
            f'cursor="{cursor}">{escape(token)}</resumptionToken>'
        )
    else:
        parts.append(
            f'    <resumptionToken completeListSize="{len(records)}" '
            f'cursor="{cursor}"></resumptionToken>'
        )
    parts.append("  </ListRecords>")
    return "\n".join(parts)


def build_list_records(params: dict[str, list[str]]) -> tuple[int, bytes]:
    def single(name: str) -> str | None:
        values = params.get(name)
        return values[0] if values else None

    token = single("resumptionToken")
    scenario: str | None = None
    cursor = 0
    if token is not None:
        try:
            cursor, scenario = decode_token(token)
        except Exception:
            return 400, _error_envelope(
                "badResumptionToken", "the resumptionToken is unknown or has expired"
            )
        if scenario == "expired_token" and not _fault_already_fired("expired_token"):
            # Simulate a server that lost its token state (restart/TTL): the
            # client must restart the original interval and deduplicate.
            return 400, _error_envelope(
                "badResumptionToken", "the resumptionToken has expired"
            )
    else:
        if single("metadataPrefix") != "oai_dc":
            return 400, _error_envelope(
                "cannotDisseminateFormat", "only metadataPrefix=oai_dc is supported"
            )
        scenario = single("demo_scenario")
        cursor = 0

    records = _dataset_for(scenario)
    if cursor >= len(records):
        return 400, _error_envelope(
            "badResumptionToken", "cursor past end of list"
        )

    if scenario == "broken_stream" and cursor > 0 and not _fault_already_fired(
        "broken_stream"
    ):
        # Deliberately truncated, non-well-formed XML.  Nothing partial is
        # committed by the harvester; the next attempt streams clean XML.
        broken = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<OAI-PMH xmlns="{OAI_NS}">\n'
            "  <ListRecords>\n"
            "    <record>\n      <header>\n        <identifier>ocr-003"
        )
        return 200, broken.encode("utf-8")

    body = _list_records_xml(scenario, cursor, len(records))
    return 200, _envelope("ListRecords", body)


def build_identify() -> bytes:
    body = (
        "  <Identify>\n"
        "    <repositoryName>Demo OAI-PMH Source for the OCR Archive</repositoryName>\n"
        "    <baseURL>http://demo-oai:8010/oai</baseURL>\n"
        "    <protocolVersion>2.0</protocolVersion>\n"
        "    <adminEmail>demo@example.invalid</adminEmail>\n"
        "    <earliestDatestamp>2025-01-01T00:00:00Z</earliestDatestamp>\n"
        "    <deletedRecord>transient</deletedRecord>\n"
        "    <granularity>YYYY-MM-DDThh:mm:ssZ</granularity>\n"
        "  </Identify>"
    )
    return _envelope("Identify", body)


class OaiRequestHandler(BaseHTTPRequestHandler):
    server_version = "DemoOAI/1.0"

    def _send(self, status: int, payload: bytes, content_type: str = "application/xml") -> None:
        self.send_response(status)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.rstrip("/") not in ("/oai", "/oai/"):
            self._send(404, _error_envelope("badVerb", "use /oai?verb=..."))
            return
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        verb = (params.get("verb") or [""])[0]
        if verb == "Identify":
            self._send(200, build_identify())
        elif verb == "ListRecords":
            status, payload = build_list_records(params)
            self._send(status, payload)
        else:
            self._send(400, _error_envelope("badVerb", f"unsupported verb {verb!r}"))

    def do_POST(self) -> None:  # noqa: N802
        self._send(405, _error_envelope("badVerb", "POST is not supported"))

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        # Keep container logs compact and clearly attributed.
        print(f"[demo-oai] {self.address_string()} {fmt % args}", flush=True)


def serve(host: str = "0.0.0.0", port: int = 8010) -> None:
    server = ThreadingHTTPServer((host, port), OaiRequestHandler)
    print(f"[demo-oai] listening on http://{host}:{port}/oai", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    import os

    serve(
        host=os.environ.get("DEMO_OAI_HOST", "0.0.0.0"),
        port=int(os.environ.get("DEMO_OAI_PORT", "8010")),
    )
