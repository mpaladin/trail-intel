from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from trailintel.matching import canonical_name


class BetrailLookupError(RuntimeError):
    """Raised when Betrail catalog fetch fails or returns invalid data."""


@dataclass(slots=True)
class BetrailMatch:
    query_name: str
    matched_name: str
    betrail_score: float | None
    profile_url: str | None
    match_score: float
    raw: dict[str, Any]


@dataclass(slots=True)
class BetrailCatalogEntry:
    name: str
    betrail_score: float
    profile_url: str | None
    raw: dict[str, Any]


class BetrailClient:
    BASE_URL = "https://www.betrail.run"
    PAGE_SIZE = 25

    def __init__(self, timeout: int = 15, cookie: str | None = None) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self._cookie = cookie
        self.last_lookup_used_cookie_fallback = False
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Referer": f"{self.BASE_URL}/rankings/level/all",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/147.0.0.0 Safari/537.36"
                ),
                "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
            }
        )

    @staticmethod
    def _get_string(item: dict[str, Any], *keys: str) -> str:
        for key in keys:
            raw = item.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
        return ""

    @classmethod
    def _is_cloudflare_block_response(cls, response: requests.Response) -> bool:
        text = (response.text or "").casefold()
        if response.status_code == 403 and (
            "just a moment" in text or "cloudflare" in text or response.headers.get("cf-mitigated")
        ):
            return True
        return False

    def _request_headers(self, *, cookie: str | None) -> dict[str, str]:
        headers = dict(self.session.headers)
        if cookie:
            headers["Cookie"] = cookie
        return headers

    def _fetch_page(self, offset: int, *, cookie: str | None) -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.BASE_URL}/api/score/full/level/{offset}/scratch/ALL/ALL",
            headers=self._request_headers(cookie=cookie),
            timeout=self.timeout,
        )

        if response.status_code >= 400:
            if self._is_cloudflare_block_response(response):
                raise BetrailLookupError("Betrail request blocked by Cloudflare.")
            raise BetrailLookupError(f"Betrail catalog failed with HTTP {response.status_code}.")

        try:
            payload = response.json()
        except ValueError as exc:
            if self._is_cloudflare_block_response(response):
                raise BetrailLookupError("Betrail request blocked by Cloudflare.") from exc
            raise BetrailLookupError("Betrail catalog response was not valid JSON.") from exc

        if not isinstance(payload, list):
            raise BetrailLookupError("Betrail catalog payload was not a list.")
        return [item for item in payload if isinstance(item, dict)]

    def _extract_name(self, item: dict[str, Any]) -> str:
        runner = item.get("runner")
        if not isinstance(runner, dict):
            runner = {}
        title = self._get_string(runner, "display_title", "title")
        if title:
            return title
        first = self._get_string(runner, "firstname")
        last = self._get_string(runner, "lastname")
        return " ".join(part for part in (first, last) if part).strip()

    def _build_profile_url(self, item: dict[str, Any]) -> str | None:
        runner = item.get("runner")
        if not isinstance(runner, dict):
            return None
        alias = self._get_string(runner, "alias")
        if not alias:
            return None
        return f"{self.BASE_URL}/runner/{alias}/overview"

    def _to_catalog_entry(self, item: dict[str, Any]) -> BetrailCatalogEntry | None:
        name = self._extract_name(item)
        if not name:
            return None

        level_raw = item.get("level")
        if level_raw in (None, ""):
            return None
        try:
            score = float(level_raw) / 100.0
        except (TypeError, ValueError):
            return None

        return BetrailCatalogEntry(
            name=name,
            betrail_score=score,
            profile_url=self._build_profile_url(item),
            raw=item,
        )

    def _fetch_catalog_above_threshold(self, *, threshold: float, cookie: str | None) -> list[BetrailCatalogEntry]:
        deduped: dict[str, BetrailCatalogEntry] = {}
        offset = 0

        for _ in range(500):
            rows = self._fetch_page(offset, cookie=cookie)
            if not rows:
                break

            saw_below_threshold = False
            for item in rows:
                entry = self._to_catalog_entry(item)
                if entry is None:
                    continue
                if entry.betrail_score <= threshold:
                    saw_below_threshold = True
                    continue
                key = canonical_name(entry.name)
                current = deduped.get(key)
                if current is None or entry.betrail_score > current.betrail_score:
                    deduped[key] = entry

            if saw_below_threshold or len(rows) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE

        entries = list(deduped.values())
        entries.sort(key=lambda item: item.betrail_score, reverse=True)
        return entries

    def fetch_catalog_above_threshold(self, threshold: float) -> list[BetrailCatalogEntry]:
        self.last_lookup_used_cookie_fallback = False
        try:
            return self._fetch_catalog_above_threshold(threshold=threshold, cookie=None)
        except BetrailLookupError as exc:
            if not self._cookie:
                raise
            try:
                entries = self._fetch_catalog_above_threshold(threshold=threshold, cookie=self._cookie)
            except BetrailLookupError as fallback_exc:
                raise BetrailLookupError(f"{exc}; cookie retry failed: {fallback_exc}") from fallback_exc
            self.last_lookup_used_cookie_fallback = True
            return entries
