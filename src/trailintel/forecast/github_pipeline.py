from __future__ import annotations

import json
import re
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from trailintel.forecast.time_utils import parse_duration
from trailintel.github_pipeline import normalize_slug_text, validate_public_https_url

FIELD_ALIASES = {
    "route name": "route_name",
    "gpx url": "gpx_url",
    "start date": "start_date",
    "start time": "start_time",
    "timezone": "timezone_name",
    "duration": "duration",
    "notes": "notes",
}
URL_RE = re.compile(r'https?://[^\s<>()"\']+')


@dataclass(slots=True)
class ForecastRequest:
    route_name: str
    gpx_url: str
    start_date: str
    start_time: str
    timezone_name: str
    duration: str
    notes: str = ""

    @property
    def route_slug(self) -> str:
        return normalize_slug_text(self.route_name)

    @property
    def start_value(self) -> str:
        return f"{self.start_date}T{self.start_time}"


@dataclass(slots=True)
class ForecastPublishResult:
    route_slug: str
    report_dir: str
    latest_dir: str
    report_url: str
    latest_url: str
    png_url: str
    gpx_url: str
    json_url: str


def _clean_issue_value(value: str) -> str:
    text = value.strip()
    if not text or text == "_No response_":
        return ""
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    filtered = [line for line in lines if not line.lstrip().startswith("<!--")]
    return "\n".join(filtered).strip()


def _parse_issue_sections(body: str) -> dict[str, str]:
    sections: dict[str, str] = {}
    current_label: str | None = None
    buffer: list[str] = []

    def flush() -> None:
        nonlocal current_label, buffer
        if current_label is None:
            buffer = []
            return
        cleaned = _clean_issue_value("\n".join(buffer))
        if cleaned:
            sections[current_label] = cleaned
        buffer = []

    for line in body.splitlines():
        if line.startswith("### "):
            flush()
            current_label = FIELD_ALIASES.get(line[4:].strip().casefold())
            continue
        if current_label is not None:
            buffer.append(line)
    flush()
    return sections


def _validate_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid start date: {value}") from exc
    return parsed.isoformat()


def _validate_time(value: str) -> str:
    try:
        parsed = time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid start time: {value}") from exc
    return (
        parsed.isoformat(timespec="seconds")
        if parsed.second
        else parsed.isoformat(timespec="minutes")
    )


def _validate_timezone(value: str) -> str:
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Invalid timezone: {value}") from exc
    return value


def parse_issue_form(body: str) -> ForecastRequest:
    sections = _parse_issue_sections(body)
    route_name = sections.get("route_name", "").strip()
    start_date = sections.get("start_date", "").strip()
    start_time = sections.get("start_time", "").strip()
    timezone_name = sections.get("timezone_name", "").strip()
    duration = sections.get("duration", "").strip()
    if not route_name:
        raise ValueError("Issue form is missing Route Name.")
    if not start_date or not start_time or not timezone_name or not duration:
        raise ValueError(
            "Issue form is missing Start Date, Start Time, Timezone, or Duration."
        )

    return ForecastRequest(
        route_name=route_name,
        gpx_url=sections.get("gpx_url", "").strip(),
        start_date=_validate_date(start_date),
        start_time=_validate_time(start_time),
        timezone_name=_validate_timezone(timezone_name),
        duration=duration if parse_duration(duration) else duration,
        notes=sections.get("notes", "").strip(),
    )


def extract_urls(text: str) -> list[str]:
    urls = [match.group(0).rstrip(".,)") for match in URL_RE.finditer(text)]
    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    return deduped


def looks_like_zip_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    return path.endswith(".zip") or ".zip/" in path or ".zip?" in url.lower()


def resolve_gpx_source_url(request: ForecastRequest, issue_body: str) -> str:
    if request.gpx_url.strip():
        return request.gpx_url.strip()

    zip_urls = [url for url in extract_urls(issue_body) if looks_like_zip_url(url)]
    if len(zip_urls) == 1:
        return zip_urls[0]
    if len(zip_urls) > 1:
        raise ValueError(
            "Multiple ZIP attachment URLs were found in the issue. Keep only one ZIP attachment or paste the GPX URL directly."
        )
    raise ValueError(
        "No GPX URL was provided and no ZIP attachment URL was found in the issue body."
    )


def _download_response_content(url: str, github_token: str | None) -> tuple[bytes, str]:
    import requests

    attempts = []
    if github_token:
        attempts.append(
            {
                "Authorization": f"Bearer {github_token}",
                "Accept": "application/octet-stream",
            }
        )
    attempts.append({})

    last_error: Exception | None = None
    for headers in attempts:
        try:
            response = requests.get(url, headers=headers or None, timeout=60)
            response.raise_for_status()
            return response.content, response.headers.get("Content-Type", "")
        except Exception as exc:  # pragma: no cover - exercised indirectly in workflow
            last_error = exc
    if last_error is None:
        raise ValueError(f"Could not download source URL: {url}")
    raise ValueError(f"Could not download source URL: {last_error}") from last_error


def _write_gpx_bytes(content: bytes, *, output_dir: str | Path) -> Path:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "route.gpx"
    path.write_bytes(content)
    return path


def _extract_single_gpx_from_zip(content: bytes, *, output_dir: str | Path) -> Path:
    try:
        with zipfile.ZipFile(BytesIO(content)) as archive:
            gpx_names = [
                name
                for name in archive.namelist()
                if not name.endswith("/") and name.lower().endswith(".gpx")
            ]
            if len(gpx_names) != 1:
                raise ValueError("ZIP attachment must contain exactly one .gpx file.")
            with archive.open(gpx_names[0]) as handle:
                return _write_gpx_bytes(handle.read(), output_dir=output_dir)
    except zipfile.BadZipFile as exc:
        raise ValueError("Downloaded source is not a valid ZIP archive.") from exc


def download_gpx_source(
    *,
    source_url: str,
    output_dir: str | Path,
    github_token: str | None = None,
) -> Path:
    validated_url = validate_public_https_url(source_url, label="GPX URL")
    content, content_type = _download_response_content(validated_url, github_token)
    if (
        zipfile.is_zipfile(BytesIO(content))
        or looks_like_zip_url(validated_url)
        or "application/zip" in content_type
    ):
        return _extract_single_gpx_from_zip(content, output_dir=output_dir)

    stripped = content.lstrip()
    if (
        validated_url.lower().endswith(".gpx")
        or stripped.startswith(b"<?xml")
        or b"<gpx" in stripped[:256].lower()
    ):
        return _write_gpx_bytes(content, output_dir=output_dir)

    raise ValueError(
        "Downloaded source is neither a GPX file nor a ZIP containing one GPX."
    )


def publish_forecast_bundle(
    *,
    source_dir: str | Path,
    pages_root: str | Path,
    request: ForecastRequest,
    published_at: datetime | None = None,
    base_url: str | None = None,
) -> ForecastPublishResult:
    from trailintel.forecast.site import publish_forecast_bundle_to_site

    timestamp = (published_at or datetime.now(UTC)).astimezone(UTC)
    published = publish_forecast_bundle_to_site(
        source_dir=source_dir,
        pages_root=pages_root,
        route_name=request.route_name,
        gpx_url=request.gpx_url,
        start_time=f"{request.start_date}T{request.start_time}",
        timezone_name=request.timezone_name,
        duration=request.duration,
        notes=request.notes,
        published_at=timestamp,
    )
    root = (base_url or "").rstrip("/")

    def to_url(path: str) -> str:
        return f"{root}/{path}" if root else path

    return ForecastPublishResult(
        route_slug=request.route_slug,
        report_dir=published["report_dir"],
        latest_dir=published["latest_dir"],
        report_url=to_url(published["report_path"]),
        latest_url=to_url(published["latest_path"]),
        png_url=to_url(published["png_path"]),
        gpx_url=to_url(published["gpx_path"]),
        json_url=to_url(published["json_path"]),
    )


def request_to_payload(
    request: ForecastRequest, *, source_url: str | None = None
) -> dict[str, object]:
    payload = asdict(request)
    payload["route_slug"] = request.route_slug
    if source_url is not None:
        payload["source_url"] = source_url
    return payload


def payload_to_request(payload: dict[str, object]) -> ForecastRequest:
    source_url = str(payload.get("source_url", "")).strip()
    return ForecastRequest(
        route_name=str(payload.get("route_name", "")).strip(),
        gpx_url=source_url or str(payload.get("gpx_url", "")).strip(),
        start_date=str(payload.get("start_date", "")).strip(),
        start_time=str(payload.get("start_time", "")).strip(),
        timezone_name=str(payload.get("timezone_name", "")).strip(),
        duration=str(payload.get("duration", "")).strip(),
        notes=str(payload.get("notes", "")).strip(),
    )


def to_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=False)
