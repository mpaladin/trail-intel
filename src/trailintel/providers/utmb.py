from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import requests

from trailintel.matching import (
    canonical_name,
    is_strong_person_name_match,
    match_score,
    search_name_variants,
)


@dataclass(slots=True)
class UtmbMatch:
    query_name: str
    matched_name: str
    utmb_index: float | None
    profile_url: str | None
    match_score: float
    raw: dict[str, Any]
    source: str = "live"


@dataclass(slots=True)
class UtmbCatalogEntry:
    name: str
    utmb_index: float
    profile_url: str | None
    raw: dict[str, Any]


class UtmbClient:
    BASE_URL = "https://api.utmb.world/search/runners"

    def __init__(self, timeout: int = 15) -> None:
        self.timeout = timeout
        self.session = requests.Session()

    def _to_profile_url(self, uri: str | None) -> str | None:
        if not uri:
            return None
        uri_text = str(uri).strip()
        if uri_text.startswith("http"):
            return uri_text
        if uri_text.startswith("runner/") or uri_text.startswith("/runner/"):
            return f"https://utmb.world/{uri_text.lstrip('/')}"
        if re.match(r"^\d+\..+", uri_text):
            return f"https://utmb.world/runner/{uri_text}"
        return f"https://utmb.world/{uri_text.lstrip('/')}"

    def _to_catalog_entry(self, runner: dict[str, Any]) -> UtmbCatalogEntry | None:
        name = str(runner.get("fullname", "")).strip()
        raw_ip = runner.get("ip")
        if raw_ip in (None, "") or not name:
            return None
        return UtmbCatalogEntry(
            name=name,
            utmb_index=float(raw_ip),
            profile_url=self._to_profile_url(runner.get("uri")),
            raw=runner,
        )

    def _fetch_runners(self, search_text: str, *, limit: int) -> list[dict[str, Any]]:
        params = {
            "category": "general",
            "search": search_text,
            "offset": 0,
            "limit": limit,
        }
        response = self.session.get(self.BASE_URL, params=params, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        runners = data.get("runners", []) or []
        return [runner for runner in runners if isinstance(runner, dict)]

    def _search_candidates(self, name: str, *, limit: int) -> list[UtmbMatch]:
        candidates: list[UtmbMatch] = []
        seen: set[tuple[str, str, str]] = set()

        for query in search_name_variants(name):
            for runner in self._fetch_runners(query, limit=limit):
                runner_name = str(runner.get("fullname", "")).strip()
                if not runner_name:
                    continue
                profile_url = self._to_profile_url(runner.get("uri")) or ""
                ip_value = str(runner.get("ip", ""))
                dedupe_key = (profile_url, runner_name.casefold(), ip_value)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                ip = runner.get("ip")
                index = float(ip) if ip not in (None, "") else None
                candidates.append(
                    UtmbMatch(
                        query_name=name,
                        matched_name=runner_name,
                        utmb_index=index,
                        profile_url=profile_url or None,
                        match_score=match_score(name, runner_name),
                        raw=runner,
                    )
                )

        return candidates

    def search_same_name_candidates(
        self, name: str, limit: int = 10
    ) -> list[UtmbMatch]:
        candidates = self._search_candidates(name, limit=limit)
        if not candidates:
            return []

        strong_candidates = [
            item
            for item in candidates
            if is_strong_person_name_match(name, item.matched_name)
        ]
        strong_candidates = [
            UtmbMatch(
                query_name=item.query_name,
                matched_name=item.matched_name,
                utmb_index=item.utmb_index,
                profile_url=item.profile_url,
                match_score=item.match_score,
                raw=item.raw,
                source="live",
            )
            for item in strong_candidates
        ]

        if not strong_candidates:
            return []

        best = max(strong_candidates, key=lambda item: item.match_score)
        target_key = canonical_name(best.matched_name)
        same_name = [
            item
            for item in strong_candidates
            if canonical_name(item.matched_name) == target_key
        ]
        same_name.sort(
            key=lambda item: (
                item.utmb_index if item.utmb_index is not None else -1.0,
                item.match_score,
            ),
            reverse=True,
        )
        return same_name

    def search(self, name: str, limit: int = 10) -> UtmbMatch | None:
        candidates = self.search_same_name_candidates(name, limit=limit)
        if not candidates:
            return None
        return candidates[0]

    def fetch_catalog_above_threshold(
        self,
        threshold: float,
        *,
        page_size: int = 100,
        max_pages: int = 120,
    ) -> list[UtmbCatalogEntry]:
        entries: list[UtmbCatalogEntry] = []
        offset = 0

        for _ in range(max_pages):
            response = self.session.get(
                self.BASE_URL,
                params={
                    "category": "general",
                    "limit": page_size,
                    "offset": offset,
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
            runners = data.get("runners", []) or []
            if not runners:
                break

            page_indexes: list[float] = []
            for runner in runners:
                entry = self._to_catalog_entry(runner)
                if not entry:
                    continue
                page_indexes.append(entry.utmb_index)
                if entry.utmb_index > threshold:
                    entries.append(entry)

            if not page_indexes:
                break
            if min(page_indexes) <= threshold:
                # Sorted descending by index; next pages will not improve threshold filter.
                break

            offset += page_size

        deduped: dict[str, UtmbCatalogEntry] = {}
        for entry in entries:
            key = canonical_name(entry.name)
            current = deduped.get(key)
            if current is None or entry.utmb_index > current.utmb_index:
                deduped[key] = entry
        return list(deduped.values())
