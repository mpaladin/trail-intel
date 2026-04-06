from __future__ import annotations

import base64
from dataclasses import dataclass
import json
import re
import time
from typing import Any

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import requests
from bs4 import BeautifulSoup

from trailintel.cache_store import LookupCacheStore
from trailintel.matching import (
    canonical_name,
    is_strong_person_name_match,
    match_score,
    search_name_variants,
)


class ItraLookupError(RuntimeError):
    """Raised when ITRA lookup fails for non-name reasons (network/WAF/etc.)."""


@dataclass(slots=True)
class ItraMatch:
    query_name: str
    matched_name: str
    itra_score: float | None
    profile_url: str | None
    match_score: float
    raw: dict[str, Any]
    source: str = "live"


@dataclass(slots=True)
class ItraCatalogEntry:
    name: str
    itra_score: float
    profile_url: str | None
    raw: dict[str, Any]


class ItraClient:
    BASE_URL = "https://itra.run"
    SEARCH_PATH = "/api/runner/findByName"
    SEARCH_PATH_AUTH = "/api/runner/find"
    RANKING_PAGE = "/Runners/Ranking"
    RETRYABLE_STATUSES = {403, 429, 503}
    MAX_POST_ATTEMPTS = 3
    RETRY_BASE_DELAY_SECONDS = 0.25

    def __init__(
        self,
        timeout: int = 15,
        cookie: str | None = None,
        *,
        cache_store: LookupCacheStore | None = None,
        use_cache: bool = True,
        force_refresh: bool = False,
    ) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self._csrf_token: str | None = None
        self.cache_store = cache_store
        self.use_cache = use_cache and cache_store is not None
        self.force_refresh = force_refresh
        self._cookie = cookie
        self.last_lookup_used_cache = False
        self.last_lookup_stale_fallback = False
        self.last_lookup_used_cookie_fallback = False
        self._auth_scope = "auth" if cookie else "public"
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Referer": f"{self.BASE_URL}{self.RANKING_PAGE}",
            }
        )
        if cookie:
            self.session.headers.update({"Cookie": cookie})

    def _cache_auth_scope(self) -> str:
        return self._auth_scope

    @staticmethod
    def _get_string(item: dict[str, Any], *keys: str) -> str:
        for key in keys:
            raw = item.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
        return ""

    def _bootstrap(self) -> None:
        if self._csrf_token:
            return
        response = self.session.get(self.BASE_URL, timeout=self.timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        token_input = soup.select_one("input[name='__RequestVerificationToken']")
        if not token_input:
            raise ItraLookupError("Could not obtain ITRA CSRF token.")
        token = token_input.get("value")
        if not token:
            raise ItraLookupError("ITRA CSRF token was empty.")
        self._csrf_token = token

    @staticmethod
    def _extract_name(item: dict[str, Any]) -> str:
        first = ItraClient._get_string(item, "firstName", "FirstName", "Fname_orig")
        last = ItraClient._get_string(item, "lastName", "LastName", "Lname_orig")
        combined = f"{first} {last}".strip()
        if combined:
            return combined
        return ItraClient._get_string(item, "name", "fullName", "fullname")

    @staticmethod
    def _extract_score(item: dict[str, Any]) -> float | None:
        for key in ("pi", "Pi", "itraScore", "score", "itra", "points"):
            raw = item.get(key)
            if raw in (None, ""):
                continue
            try:
                return float(raw)
            except (TypeError, ValueError):
                continue
        return None

    def _build_profile_url(self, item: dict[str, Any]) -> str | None:
        runner_id = (
            item.get("runnerId")
            or item.get("RunnerId")
            or item.get("id")
            or item.get("Id_runner")
        )
        first = self._get_string(item, "firstName", "FirstName", "Fname_orig")
        last = self._get_string(item, "lastName", "LastName", "Lname_orig")
        if runner_id and first and last:
            return f"{self.BASE_URL}/RunnerSpace/{last}.{first}/{runner_id}"
        for key in ("url", "uri", "profileUrl"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                if value.startswith("http"):
                    return value
                return f"{self.BASE_URL}{value}"
        return None

    @staticmethod
    def _extract_results(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if not isinstance(payload, dict):
            return []
        for key in ("results", "Results", "data", "Data", "runners", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

    @staticmethod
    def _is_encrypted_payload(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        return all(isinstance(payload.get(key), str) for key in ("response1", "response2", "response3"))

    @staticmethod
    def _is_cloudfront_block_response(response: requests.Response | None) -> bool:
        if response is None:
            return False
        if response.status_code != 403:
            return False
        text = (response.text or "").casefold()
        return "cloudfront" in text and ("request blocked" in text or "generated by cloudfront" in text)

    @classmethod
    def _is_retryable_status(cls, status: int) -> bool:
        return status in cls.RETRYABLE_STATUSES

    @staticmethod
    def _depad_pkcs7(padded: bytes) -> bytes:
        if not padded:
            raise ItraLookupError("ITRA encrypted response was empty.")
        pad_length = padded[-1]
        if pad_length < 1 or pad_length > 16:
            raise ItraLookupError("ITRA encrypted response had invalid padding.")
        if padded[-pad_length:] != bytes([pad_length]) * pad_length:
            raise ItraLookupError("ITRA encrypted response had invalid padding.")
        return padded[:-pad_length]

    def _decrypt_payload_json(self, payload: dict[str, Any]) -> Any:
        try:
            ciphertext = base64.b64decode(payload["response1"], validate=True)
            iv = base64.b64decode(payload["response2"], validate=True)
            key = base64.b64decode(payload["response3"], validate=True)
        except Exception as exc:
            raise ItraLookupError("ITRA encrypted payload could not be decoded.") from exc

        if len(iv) != 16:
            raise ItraLookupError("ITRA encrypted payload had an invalid IV size.")
        if len(key) not in {16, 24, 32}:
            raise ItraLookupError("ITRA encrypted payload had an invalid key size.")

        try:
            cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
            decryptor = cipher.decryptor()
            padded = decryptor.update(ciphertext) + decryptor.finalize()
        except Exception as exc:
            raise ItraLookupError("ITRA encrypted payload decryption failed.") from exc

        plaintext = self._depad_pkcs7(padded)
        try:
            decoded = plaintext.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ItraLookupError("ITRA encrypted payload could not be decoded as UTF-8.") from exc
        try:
            return json.loads(decoded)
        except json.JSONDecodeError as exc:
            raise ItraLookupError("ITRA encrypted payload was not valid JSON.") from exc

    def _post_search(
        self,
        path: str,
        *,
        data: dict[str, Any],
        headers: dict[str, str],
    ) -> list[dict[str, Any]]:
        last_status: int | None = None
        for attempt in range(1, self.MAX_POST_ATTEMPTS + 1):
            response = self.session.post(
                f"{self.BASE_URL}{path}",
                data=data,
                headers=headers,
                timeout=self.timeout,
            )
            last_status = response.status_code

            if response.status_code >= 400:
                if (
                    self._is_retryable_status(response.status_code)
                    and attempt < self.MAX_POST_ATTEMPTS
                ):
                    time.sleep(self.RETRY_BASE_DELAY_SECONDS * attempt)
                    continue
                response.raise_for_status()

            try:
                payload = response.json()
            except ValueError as exc:
                if self._is_cloudfront_block_response(response):
                    if attempt < self.MAX_POST_ATTEMPTS:
                        time.sleep(self.RETRY_BASE_DELAY_SECONDS * attempt)
                        continue
                    raise ItraLookupError("ITRA request blocked by CloudFront.") from exc
                raise ItraLookupError("ITRA response was not valid JSON.") from exc

            if self._is_encrypted_payload(payload):
                payload = self._decrypt_payload_json(payload)
            return self._extract_results(payload)

        if last_status is not None:
            raise ItraLookupError(f"ITRA lookup failed with HTTP {last_status}.")
        raise ItraLookupError("ITRA lookup failed before receiving a response.")

    def _search_variant(
        self,
        *,
        query: str,
        headers: dict[str, str],
    ) -> tuple[list[dict[str, Any]], bool, list[str]]:
        variant_results: list[dict[str, Any]] = []
        failures: list[str] = []
        had_success = False

        for path, data in (
            (
                self.SEARCH_PATH_AUTH,
                {"name": query, "start": 1, "count": 10, "echoToken": "0.1"},
            ),
            (self.SEARCH_PATH, {"name": query}),
        ):
            try:
                payload = self._post_search(path, data=data, headers=headers)
                had_success = True
                if payload:
                    variant_results.extend(payload)
            except ItraLookupError as exc:
                failures.append(f"{path} {exc}")
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "?"
                if self._is_cloudfront_block_response(exc.response):
                    failures.append(f"{path} blocked by CloudFront (HTTP {status})")
                else:
                    failures.append(f"{path} HTTP {status}")
            except requests.RequestException as exc:
                failures.append(f"{path} error: {exc.__class__.__name__}")

        return variant_results, had_success, failures

    def _search_candidates(self, name: str) -> list[ItraMatch]:
        try:
            self._bootstrap()
            headers = {
                "X-CSRF-TOKEN": self._csrf_token or "",
                "X-Requested-With": "XMLHttpRequest",
            }

            rows: list[dict[str, Any]] = []
            failures: list[str] = []
            had_success = False

            for query in search_name_variants(name):
                variant_rows, variant_success, variant_failures = self._search_variant(
                    query=query,
                    headers=headers,
                )
                rows.extend(variant_rows)
                failures.extend(variant_failures)
                had_success = had_success or variant_success

            if not rows and failures and not had_success:
                raise ItraLookupError("ITRA lookup failed: " + "; ".join(failures))
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            raise ItraLookupError(f"ITRA lookup failed with HTTP {status}.") from exc
        except requests.RequestException as exc:
            raise ItraLookupError(f"ITRA lookup request failed: {exc}") from exc

        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for item in rows:
            if not isinstance(item, dict):
                continue
            matched_name = self._extract_name(item)
            if not matched_name:
                continue
            runner_id = str(item.get("runnerId") or item.get("id") or item.get("Id_runner") or "")
            profile_url = self._build_profile_url(item) or ""
            dedupe_key = (runner_id, profile_url, matched_name.casefold())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            deduped.append(item)

        candidates: list[ItraMatch] = []
        for item in deduped:
            matched_name = self._extract_name(item)
            candidates.append(
                ItraMatch(
                    query_name=name,
                    matched_name=matched_name,
                    itra_score=self._extract_score(item),
                    profile_url=self._build_profile_url(item),
                    match_score=match_score(name, matched_name),
                    raw=item,
                )
            )
        return candidates

    @staticmethod
    def _serialize_candidates(candidates: list[ItraMatch]) -> str:
        rows = [
            {
                "matched_name": candidate.matched_name,
                "itra_score": candidate.itra_score,
                "profile_url": candidate.profile_url,
                "match_score": candidate.match_score,
            }
            for candidate in candidates
        ]
        return json.dumps(rows, ensure_ascii=False)

    @staticmethod
    def _deserialize_candidates(name: str, payload_json: str, *, source: str) -> list[ItraMatch]:
        if not payload_json.strip():
            return []
        try:
            rows = json.loads(payload_json)
        except json.JSONDecodeError:
            return []
        if not isinstance(rows, list):
            return []

        parsed: list[ItraMatch] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            matched_name = str(row.get("matched_name", "")).strip()
            if not matched_name:
                continue

            score_raw = row.get("itra_score")
            itra_score: float | None
            if score_raw in (None, ""):
                itra_score = None
            else:
                try:
                    itra_score = float(score_raw)
                except (TypeError, ValueError):
                    itra_score = None

            match_raw = row.get("match_score")
            try:
                match_value = float(match_raw)
            except (TypeError, ValueError):
                match_value = match_score(name, matched_name)

            profile = row.get("profile_url")
            profile_url = str(profile).strip() if isinstance(profile, str) else None
            parsed.append(
                ItraMatch(
                    query_name=name,
                    matched_name=matched_name,
                    itra_score=itra_score,
                    profile_url=profile_url,
                    match_score=match_value,
                    raw={},
                    source=source,
                )
            )
        parsed.sort(
            key=lambda item: (
                item.itra_score if item.itra_score is not None else -1.0,
                item.match_score,
            ),
            reverse=True,
        )
        return parsed

    def _get_cached_candidates(self, name: str) -> tuple[list[ItraMatch] | None, bool]:
        if not self.use_cache or self.cache_store is None or self.force_refresh:
            return None, False
        try:
            entry = self.cache_store.get_lookup(
                provider="itra",
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

    def _put_cached_candidates(self, name: str, candidates: list[ItraMatch]) -> None:
        if not self.use_cache or self.cache_store is None:
            return
        try:
            status = "success" if candidates else "miss"
            self.cache_store.put_lookup(
                provider="itra",
                query_name=name,
                auth_scope=self._cache_auth_scope(),
                status=status,
                payload_json=self._serialize_candidates(candidates),
            )
        except Exception:
            return

    def _should_try_anonymous_fallback(self, error: ItraLookupError) -> bool:
        if not self._cookie:
            return False
        message = str(error).casefold()
        if "cloudfront" in message or "request blocked" in message:
            return True
        if "csrf" in message:
            return True
        if "/api/runner/find" in message and any(
            f"http {status}" in message for status in (400, 401, 403, 429, 503)
        ):
            return True
        return False

    def search_same_name_candidates(self, name: str) -> list[ItraMatch]:
        self.last_lookup_used_cache = False
        self.last_lookup_stale_fallback = False
        self.last_lookup_used_cookie_fallback = False

        stale_cached_candidates: list[ItraMatch] | None = None
        cached_candidates, is_stale = self._get_cached_candidates(name)
        if cached_candidates is not None:
            self.last_lookup_used_cache = True
            if is_stale:
                stale_cached_candidates = cached_candidates
            else:
                return cached_candidates

        try:
            candidates = self._search_candidates(name)
            if not candidates:
                self._put_cached_candidates(name, [])
                return []
        except ItraLookupError as exc:
            recovered_with_anonymous = False
            if self._should_try_anonymous_fallback(exc):
                anonymous_client = ItraClient(
                    timeout=self.timeout,
                    cookie=None,
                    cache_store=None,
                    use_cache=False,
                    force_refresh=True,
                )
                try:
                    candidates = anonymous_client._search_candidates(name)
                    self.last_lookup_used_cookie_fallback = True
                    recovered_with_anonymous = True
                    if not candidates:
                        self._put_cached_candidates(name, [])
                        return []
                except ItraLookupError as fallback_exc:
                    exc = ItraLookupError(f"{exc}; anonymous retry failed: {fallback_exc}")

            if not recovered_with_anonymous:
                if stale_cached_candidates is not None:
                    self.last_lookup_used_cache = True
                    self.last_lookup_stale_fallback = True
                    return stale_cached_candidates
                raise exc

        strong_candidates = [
            item for item in candidates if is_strong_person_name_match(name, item.matched_name)
        ]
        strong_candidates = [
            ItraMatch(
                query_name=item.query_name,
                matched_name=item.matched_name,
                itra_score=item.itra_score,
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
                item.itra_score if item.itra_score is not None else -1.0,
                item.match_score,
            ),
            reverse=True,
        )
        self._put_cached_candidates(name, same_name)
        return same_name

    def search(self, name: str) -> ItraMatch | None:
        candidates = self.search_same_name_candidates(name)
        if not candidates:
            return None
        return candidates[0]

    def fetch_public_catalog_above_threshold(self, threshold: float) -> list[ItraCatalogEntry]:
        response = self.session.get(f"{self.BASE_URL}{self.RANKING_PAGE}", timeout=self.timeout)
        response.raise_for_status()

        match = re.search(
            r"window\.allTop5Runners\s*=\s*(\[[\s\S]*?\]);",
            response.text,
            flags=re.MULTILINE,
        )
        if not match:
            raise ItraLookupError("Could not parse ITRA public ranking payload.")

        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError as exc:
            raise ItraLookupError("ITRA public ranking payload was not valid JSON.") from exc

        deduped: dict[str, ItraCatalogEntry] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue

            score_raw = item.get("Pi")
            if score_raw in (None, ""):
                continue
            try:
                score = float(score_raw)
            except (TypeError, ValueError):
                continue
            if score <= threshold:
                continue

            first = str(item.get("Fname_orig", "")).strip()
            last = str(item.get("Lname_orig", "")).strip()
            if not (first and last):
                continue

            name = f"{first} {last}".strip()
            runner_id = item.get("Id_runner")
            profile_url = (
                f"{self.BASE_URL}/RunnerSpace/{last}.{first}/{runner_id}" if runner_id else None
            )
            entry = ItraCatalogEntry(name=name, itra_score=score, profile_url=profile_url, raw=item)
            key = canonical_name(name)
            current = deduped.get(key)
            if current is None or entry.itra_score > current.itra_score:
                deduped[key] = entry

        return list(deduped.values())
