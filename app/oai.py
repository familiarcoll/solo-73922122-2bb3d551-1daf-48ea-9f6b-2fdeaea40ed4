"""Minimal OAI-PMH 2.0 client and UTC-second datestamp helpers.

Only ``ListRecords`` with ``metadataPrefix=oai_dc`` is needed by the harvester.
The client uses the standard library only and performs every blocking network
operation outside of any database transaction.
"""

from __future__ import annotations

import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .config import SETTINGS

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
DC_NS = "http://purl.org/dc/elements/1.1/"

_NS = {"oai": OAI_NS, "oai_dc": OAI_DC_NS, "dc": DC_NS}


class HarvestError(Exception):
    """Base class for every harvest-side failure."""


class HarvestTransportError(HarvestError):
    """Connection, timeout or HTTP failure while talking to the OAI source."""


class HarvestProtocolError(HarvestError):
    """The source answered with an OAI ``error`` element."""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        super().__init__(f"{code}: {message}".rstrip(": "))


class HarvestParseError(HarvestError):
    """The response body was not well-formed OAI-PMH XML."""


@dataclass(frozen=True)
class OaiRecord:
    """One parsed ``record`` element.

    ``deleted`` records carry no metadata per OAI-PMH; the harvester turns them
    into cataloguing tombstones without touching OCR text.
    """

    identifier: str
    datestamp: str
    deleted: bool
    titles: tuple[str, ...] = field(default_factory=tuple)
    creators: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ListRecordsResponse:
    records: tuple[OaiRecord, ...]
    resumption_token: str | None


def parse_utc_second(value: str) -> datetime:
    """Parse an OAI-PMH UTC datestamp at (at most) second granularity."""

    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"invalid OAI datestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"OAI datestamp must include a timezone: {value!r}")
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def format_utc_second(moment: datetime) -> str:
    """Render a UTC datetime as an OAI-PMH second-granularity datestamp."""

    return (
        moment.astimezone(timezone.utc)
        .replace(microsecond=0)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def normalize_datestamp(value: str) -> str:
    """Return the canonical UTC-second string used for datestamp comparisons."""

    return format_utc_second(parse_utc_second(value))


def build_list_records_url(
    base_url: str,
    *,
    metadata_prefix: str,
    from_datestamp: str | None = None,
    until_datestamp: str | None = None,
    resumption_token: str | None = None,
    extra_params: dict[str, str] | None = None,
) -> str:
    params: list[tuple[str, str]] = [("verb", "ListRecords")]
    if resumption_token:
        # With a resumptionToken the other ListRecords arguments are forbidden.
        params.append(("resumptionToken", resumption_token))
    else:
        params.append(("metadataPrefix", metadata_prefix))
        if from_datestamp:
            params.append(("from", from_datestamp))
        if until_datestamp:
            params.append(("until", until_datestamp))
    if extra_params:
        for key, val in extra_params.items():
            params.append((key, val))
    separator = "&" if "?" in base_url else "?"
    return base_url + separator + urllib.parse.urlencode(params)


def _http_get(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "ocr-archive-harvester/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=SETTINGS.oai_timeout_seconds) as response:
            return response.read()
    except urllib.error.HTTPError as exc:
        # OAI-PMH signals protocol errors with an OAI <error> body even on HTTP
        # 4xx.  Hand the payload to the XML parser; only a non-parseable body
        # becomes a transport failure.
        try:
            return exc.read()
        except (OSError, ValueError) as read_exc:
            raise HarvestTransportError(
                f"OAI source returned HTTP {exc.code} and the body was unreadable: {read_exc}"
            ) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise HarvestTransportError(f"OAI source request failed: {exc}") from exc


def _parse_oai_error(root: ET.Element) -> HarvestProtocolError | None:
    error = root.find("oai:error", _NS)
    if error is None:
        return None
    code = error.attrib.get("code", "unknown")
    message = (error.text or "").strip()
    return HarvestProtocolError(code, message)


def _parse_record(element: ET.Element) -> OaiRecord:
    header = element.find("oai:header", _NS)
    if header is None:
        raise HarvestParseError("record is missing an OAI header")
    identifier_node = header.find("oai:identifier", _NS)
    datestamp_node = header.find("oai:datestamp", _NS)
    if identifier_node is None or not (identifier_node.text or "").strip():
        raise HarvestParseError("record header is missing an identifier")
    if datestamp_node is None or not (datestamp_node.text or "").strip():
        raise HarvestParseError(f"record {identifier_node.text!r} is missing a datestamp")
    identifier = identifier_node.text.strip()
    datestamp = normalize_datestamp(datestamp_node.text)
    deleted = header.attrib.get("status") == "deleted"

    titles: list[str] = []
    creators: list[str] = []
    if not deleted:
        metadata = element.find("oai:metadata", _NS)
        if metadata is None:
            raise HarvestParseError(f"record {identifier!r} is missing metadata")
        # Preserve every repeated dc:title / dc:creator, in document order.
        for node in metadata.findall("oai_dc:dc/dc:title", _NS):
            titles.append((node.text or "").strip())
        for node in metadata.findall("oai_dc:dc/dc:creator", _NS):
            creators.append((node.text or "").strip())
    return OaiRecord(
        identifier=identifier,
        datestamp=datestamp,
        deleted=deleted,
        titles=tuple(titles),
        creators=tuple(creators),
    )


def parse_list_records(payload: bytes) -> ListRecordsResponse:
    """Parse a ListRecords response body into records and a resumption token."""

    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise HarvestParseError(f"response is not well-formed XML: {exc}") from exc

    protocol_error = _parse_oai_error(root)
    if protocol_error is not None:
        raise protocol_error

    list_records = root.find("oai:ListRecords", _NS)
    if list_records is None:
        raise HarvestParseError("response is missing a ListRecords element")

    records = tuple(_parse_record(node) for node in list_records.findall("oai:record", _NS))
    token_node = list_records.find("oai:resumptionToken", _NS)
    token = token_node.text.strip() if token_node is not None and token_node.text else None
    return ListRecordsResponse(records=records, resumption_token=token)


def list_records(
    base_url: str,
    *,
    metadata_prefix: str,
    from_datestamp: str | None = None,
    until_datestamp: str | None = None,
    resumption_token: str | None = None,
    extra_params: dict[str, str] | None = None,
) -> ListRecordsResponse:
    """Issue one ListRecords request and parse the response."""

    url = build_list_records_url(
        base_url,
        metadata_prefix=metadata_prefix,
        from_datestamp=from_datestamp,
        until_datestamp=until_datestamp,
        resumption_token=resumption_token,
        extra_params=extra_params,
    )
    payload = _http_get(url)
    return parse_list_records(payload)


def catalog_fingerprint(record: OaiRecord) -> dict[str, Any]:
    """Comparable cataloguing content of a non-deleted record."""

    return {"titles": list(record.titles), "creators": list(record.creators)}
