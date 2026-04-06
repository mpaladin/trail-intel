from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any

import requests

from trailintel.cache_store import LookupCacheStore
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

    def __init__(
        self,
        timeout: int = 15,
        *,
        cache_store: LookupCacheStore | None = None,
        use_cache: bool = True,
        force_refresh: bool = False,
    ) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.cache_store = cache_store
        self.use_cache = use_cache and cache_store is not None
        self.force_refresh = force_refresh
        self.last_lookup_used_cache = False
        self.last_lookup_stale_fallback = False

    @staticmethod
    def _cache_auth_scope() -> str:
        return "public"

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

    @staticmethod
    def _serialize_candidates(candidates: list[UtmbMatch]) -> str:
        rows = [
            {
                "matched_name": candidate.matched_name,
                "utmb_index": candidate.utmb_index,
                "profile_url": candidate.profile_url,
                "match_score": candidate.match_score,
            }
            for candidate in candidates
        ]
        return json.dumps(rows, ensure_ascii=False)

    @staticmethod
    def _deserialize_candidates(name: str, payload_json: str, *, source: str) -> list[UtmbMatch]:
        if not payload_json.strip():
            return []
        try:
            rows = json.loads(payload_json)
        except json.JSONDecodeError:
            return []
        if not isinstance(rows, list):
            return []

        parsed: list[UtmbMatch] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            matched_name = str(row.get("matched_name", "")).strip()
            if not matched_name:
                continue

            index_raw = row.get("utmb_index")
            index_value: float | None
            if index_raw in (None, ""):
                index_value = None
            else:
                try:
                    index_value = float(index_raw)
                except (TypeError, ValueError):
                    index_value = None

            score_raw = row.get("match_score")
            try:
                score_value = float(score_raw)
            except (TypeError, ValueError):
                score_value = match_score(name, matched_name)

            profile = row.get("profile_url")
            profile_url = str(profile).strip() if isinstance(profile, str) else None
            parsed.append(
                UtmbMatch(
                    query_name=name,
                    matched_name=matched_name,
                    utmb_index=index_value,
                    profile_url=profile_url,
                    match_score=score_value,
                    raw={},
                    source=source,
                )
            )
        parsed.sort(
            key=lambda item: (
                item.utmb_index if item.utmb_index is not None else -1.0,
                item.match_score,
            ),
            reverse=True,
        )
        return parsed

    def _get_cached_candidates(self, name: str) -> tuple[list[UtmbMatch] | None, bool]:
        if not self.use_cache or self.cache_store is None or self.force_refresh:
            return None, False
        try:
            entry = self.cache_store.get_lookup(
                provider="utmb",
                query_name=name,
                auth_scope=self._cache_auth_scope(),
            )
        except Exception:
            return None, False
        if not entry:
            return None, False
        if entry.is_stale:
            return self._deserialize_candidates(name, entry.payload_json, source="stale_cache"), True
        return self._deserialize_candidates(name, entry.payload_json, source="cache"), False

    def _put_cached_candidates(self, name: str, candidates: list[UtmbMatch]) -> None:
        if not self.use_cache or self.cache_store is None:
            return
        try:
            status = "success" if candidates else "miss"
            self.cache_store.put_lookup(
                provider="utmb",
                query_name=name,
                auth_scope=self._cache_auth_scope(),
                status=status,
                payload_json=self._serialize_candidates(candidates),
            )
        except Exception:
            # Cache failures should not fail live lookups.
            return

    def search_same_name_candidates(self, name: str, limit: int = 10) -> list[UtmbMatch]:
        self.last_lookup_used_cache = False
        self.last_lookup_stale_fallback = False

        stale_cached_candidates: list[UtmbMatch] | None = None
        cached_candidates, is_stale = self._get_cached_candidates(name)
        if cached_candidates is not None:
            self.last_lookup_used_cache = True
            if is_stale:
                stale_cached_candidates = cached_candidates
            else:
                return cached_candidates

        try:
            candidates = self._search_candidates(name, limit=limit)
            if not candidates:
                self._put_cached_candidates(name, [])
                return []
        except requests.RequestException:
            if stale_cached_candidates is not None:
                self.last_lookup_used_cache = True
                self.last_lookup_stale_fallback = True
                return stale_cached_candidates
            raise

        strong_candidates = [
            item for item in candidates if is_strong_person_name_match(name, item.matched_name)
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
            self._put_cached_candidates(name, [])
            return []

        best = max(strong_candidates, key=lambda item: item.match_score)
        target_key = canonical_name(best.matched_name)
        same_name = [item for item in strong_candidates if canonical_name(item.matched_name) == target_key]
        same_name.sort(
            key=lambda item: (
                item.utmb_index if item.utmb_index is not None else -1.0,
                item.match_score,
            ),
            reverse=True,
        )
        self._put_cached_candidates(name, same_name)
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
