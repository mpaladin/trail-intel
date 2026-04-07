from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import re
import tomllib
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from trailintel.cache_store import MISS_TTL_DAYS, SUCCESS_TTL_DAYS, default_config_file_path
from trailintel.matching import canonical_name, is_strong_person_name_match

ATHLETE_SCHEMA_VERSION = "athlete-v1"
RUN_SCHEMA_VERSION = "run-summary-v1"
_PROVIDER_SCORE_SCALES = {
    "utmb": "1000",
    "itra": "1000",
    "betrail": "100",
}
_PROVIDERS = tuple(_PROVIDER_SCORE_SCALES.keys())


@dataclass(slots=True)
class RepoProviderObservation:
    provider: str
    status: str
    matched_name: str | None = None
    profile_url: str | None = None
    score: float | None = None
    score_scale: str | None = None
    match_confidence: float | None = None
    source_run_id: str | None = None
    checked_at: datetime | None = None
    lookup_threshold: float | None = None
    persist: bool = True


@dataclass(slots=True)
class RepoProviderLookup:
    athlete_id: str
    primary_name: str
    status: str
    matched_name: str | None
    profile_url: str | None
    score: float | None
    score_scale: str | None
    match_confidence: float | None
    provider_uid: str | None
    last_checked_at: datetime
    expires_at: datetime
    is_stale: bool
    lookup_threshold: float | None


@dataclass(slots=True)
class RepoWriteResult:
    athlete_id: str
    created: bool
    updated: bool
    provider_updates: int


def default_score_repo_path(config_path: Path | None = None) -> Path | None:
    configured = os.getenv("TRAILINTEL_SCORE_REPO")
    if configured:
        return Path(configured).expanduser()

    path = (config_path or default_config_file_path()).expanduser()
    if not path.exists():
        return None
    try:
        parsed = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None

    score_repo = parsed.get("score_repo")
    if not isinstance(score_repo, dict):
        return None

    for key in ("path", "repo_path"):
        configured_path = score_repo.get(key)
        if isinstance(configured_path, str) and configured_path.strip():
            return Path(configured_path.strip()).expanduser()
    return None


def build_run_id(*, now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(UTC)).astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{uuid4().hex[:8]}"


def provider_score_scale(provider: str) -> str:
    return _PROVIDER_SCORE_SCALES.get(provider, "1000")


def provider_uid_from_profile(provider: str, profile_url: str | None) -> str | None:
    if not profile_url:
        return None

    raw_value = str(profile_url).strip()
    if not raw_value:
        return None

    path = urlparse(raw_value).path if "://" in raw_value else raw_value
    normalized = path.strip().lstrip("/").rstrip("/")
    if not normalized:
        return None

    if provider == "utmb":
        if re.match(r"^\d+\..+", normalized):
            return f"runner/{normalized}"
        return normalized

    if provider == "itra":
        if normalized.lower().startswith("runnerspace/"):
            parts = normalized.split("/")
            if parts and parts[-1]:
                return parts[-1]
        return normalized

    if provider == "betrail":
        match = re.search(r"runner/([^/]+)/overview$", normalized)
        if match:
            return match.group(1)
        return normalized

    return normalized


def _isoformat(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def _normalize_dt(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        folded = text.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        ordered.append(text)
    return ordered


def _athlete_schema_payload() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "TrailIntel athlete score snapshot",
        "type": "object",
        "required": ["schema_version", "identity", "providers", "provenance", "created_at", "updated_at"],
        "properties": {
            "schema_version": {"const": ATHLETE_SCHEMA_VERSION},
            "identity": {
                "type": "object",
                "required": ["athlete_id", "primary_name", "canonical_name", "aliases"],
                "properties": {
                    "athlete_id": {"type": "string"},
                    "primary_name": {"type": "string"},
                    "canonical_name": {"type": "string"},
                    "aliases": {"type": "array", "items": {"type": "string"}},
                },
                "additionalProperties": True,
            },
            "providers": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "required": [
                        "status",
                        "provider_uid",
                        "matched_name",
                        "profile_url",
                        "score",
                        "score_scale",
                        "match_confidence",
                        "last_checked_at",
                        "expires_at",
                        "source_run_id",
                    ],
                    "properties": {
                        "status": {"enum": ["matched", "miss"]},
                        "provider_uid": {"type": ["string", "null"]},
                        "matched_name": {"type": ["string", "null"]},
                        "profile_url": {"type": ["string", "null"]},
                        "score": {"type": ["number", "null"]},
                        "score_scale": {"type": ["string", "null"]},
                        "match_confidence": {"type": ["number", "null"]},
                        "last_checked_at": {"type": "string"},
                        "expires_at": {"type": "string"},
                        "source_run_id": {"type": ["string", "null"]},
                        "lookup_threshold": {"type": ["number", "null"]},
                    },
                    "additionalProperties": True,
                },
            },
            "provenance": {"type": "object"},
            "created_at": {"type": "string"},
            "updated_at": {"type": "string"},
        },
        "additionalProperties": True,
    }


def _run_schema_payload() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "TrailIntel score repo run summary",
        "type": "object",
        "required": ["schema_version", "run_id", "run_kind", "created_at", "summary"],
        "properties": {
            "schema_version": {"const": RUN_SCHEMA_VERSION},
            "run_id": {"type": "string"},
            "run_kind": {"type": "string"},
            "created_at": {"type": "string"},
            "summary": {"type": "object"},
        },
        "additionalProperties": True,
    }


class AthleteScoreRepo:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser()
        self._docs: dict[str, dict[str, Any]] = {}
        self._name_index: dict[str, set[str]] = {}
        self._provider_uid_index: dict[tuple[str, str], str] = {}
        self._loaded = False

    def ensure_layout(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "athletes").mkdir(exist_ok=True)
        (self.root / "runs").mkdir(exist_ok=True)
        schema_dir = self.root / "schema"
        schema_dir.mkdir(exist_ok=True)
        self._write_if_changed(
            schema_dir / "athlete-v1.schema.json",
            self._serialize_json(_athlete_schema_payload()),
        )
        self._write_if_changed(
            schema_dir / "run-summary-v1.schema.json",
            self._serialize_json(_run_schema_payload()),
        )

    def load(self) -> None:
        self._docs = {}
        self._name_index = {}
        self._provider_uid_index = {}
        if not self.root.exists():
            self._loaded = True
            return

        athletes_dir = self.root / "athletes"
        if not athletes_dir.exists():
            self._loaded = True
            return

        for path in sorted(athletes_dir.glob("*/*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                continue
            doc = self._normalize_doc(payload, fallback_athlete_id=path.stem)
            athlete_id = str(doc["identity"]["athlete_id"])
            self._docs[athlete_id] = doc
            self._add_doc_indexes(doc)

        self._loaded = True

    def generate_run_id(self, *, now: datetime | None = None) -> str:
        return build_run_id(now=now)

    def get_provider_snapshot(
        self,
        *,
        query_name: str,
        provider: str,
        lookup_threshold: float | None = None,
    ) -> RepoProviderLookup | None:
        self._ensure_loaded()
        doc = self._resolve_doc_for_query(query_name)
        if doc is None:
            return None

        providers = doc.get("providers")
        if not isinstance(providers, dict):
            return None
        snapshot = providers.get(provider)
        if not isinstance(snapshot, dict):
            return None

        status = str(snapshot.get("status", "")).strip()
        if status not in {"matched", "miss"}:
            return None

        stored_threshold = _as_float(snapshot.get("lookup_threshold"))
        if status == "miss" and lookup_threshold is not None and stored_threshold is not None:
            if lookup_threshold < stored_threshold:
                return None

        last_checked_at = _normalize_dt(snapshot.get("last_checked_at"))
        expires_at = _normalize_dt(snapshot.get("expires_at"))
        if last_checked_at is None or expires_at is None:
            return None

        now = datetime.now(UTC)
        identity = doc.get("identity")
        primary_name = ""
        athlete_id = ""
        if isinstance(identity, dict):
            primary_name = str(identity.get("primary_name", "")).strip()
            athlete_id = str(identity.get("athlete_id", "")).strip()

        return RepoProviderLookup(
            athlete_id=athlete_id,
            primary_name=primary_name,
            status=status,
            matched_name=_text_or_none(snapshot.get("matched_name")),
            profile_url=_text_or_none(snapshot.get("profile_url")),
            score=_as_float(snapshot.get("score")),
            score_scale=_text_or_none(snapshot.get("score_scale")),
            match_confidence=_as_float(snapshot.get("match_confidence")),
            provider_uid=_text_or_none(snapshot.get("provider_uid")),
            last_checked_at=last_checked_at,
            expires_at=expires_at,
            is_stale=expires_at <= now,
            lookup_threshold=stored_threshold,
        )

    def write_athlete_observations(
        self,
        *,
        input_name: str,
        observations: list[RepoProviderObservation],
        source_run_id: str | None,
        source_kind: str,
        observed_at: datetime | None = None,
    ) -> RepoWriteResult:
        self._ensure_loaded()
        self.ensure_layout()

        now = (observed_at or datetime.now(UTC)).astimezone(UTC)
        persisted = [obs for obs in observations if self._should_persist_observation(obs)]
        doc = self._resolve_doc_for_observations(input_name=input_name, observations=persisted)
        if doc is not None and not persisted and canonical_name(input_name) in self._doc_names(doc):
            return RepoWriteResult(
                athlete_id=str(doc["identity"]["athlete_id"]),
                created=False,
                updated=False,
                provider_updates=0,
            )
        created = False
        if doc is None:
            doc = self._new_doc(primary_name=input_name, observed_at=now)
            created = True

        athlete_id = str(doc["identity"]["athlete_id"])
        old_names = self._doc_names(doc)
        old_provider_keys = self._doc_provider_keys(doc)
        providers = doc.setdefault("providers", {})
        provenance = doc.setdefault("provenance", {})
        if not isinstance(providers, dict):
            providers = {}
            doc["providers"] = providers
        if not isinstance(provenance, dict):
            provenance = {}
            doc["provenance"] = provenance

        aliases_changed = self._update_identity_aliases(doc, input_name=input_name, observations=persisted)
        provenance_changed = self._update_provenance(
            provenance=provenance,
            source_run_id=source_run_id,
            source_kind=source_kind,
        )

        provider_updates = 0
        for observation in persisted:
            snapshot = self._snapshot_from_observation(observation=observation, observed_at=now)
            existing = providers.get(observation.provider)
            if existing == snapshot:
                continue
            providers[observation.provider] = snapshot
            provider_updates += 1

        updated = created or aliases_changed or provenance_changed or provider_updates > 0
        if not updated:
            return RepoWriteResult(
                athlete_id=athlete_id,
                created=False,
                updated=False,
                provider_updates=0,
            )

        if created:
            doc["created_at"] = _isoformat(now)
        doc["updated_at"] = _isoformat(now)
        self._docs[athlete_id] = doc

        self._remove_index_entries(athlete_id=athlete_id, names=old_names, provider_keys=old_provider_keys)
        self._add_doc_indexes(doc)

        self._write_if_changed(self._athlete_path(athlete_id), self._serialize_json(doc))
        return RepoWriteResult(
            athlete_id=athlete_id,
            created=created,
            updated=True,
            provider_updates=provider_updates,
        )

    def write_run_summary(
        self,
        *,
        run_id: str,
        run_kind: str,
        summary: dict[str, Any],
        created_at: datetime | None = None,
    ) -> Path:
        self.ensure_layout()
        timestamp = (created_at or datetime.now(UTC)).astimezone(UTC)
        year_dir = self.root / "runs" / timestamp.strftime("%Y")
        year_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": RUN_SCHEMA_VERSION,
            "run_id": run_id,
            "run_kind": run_kind,
            "created_at": _isoformat(timestamp),
            "summary": summary,
        }
        path = year_dir / f"{run_id}.json"
        self._write_if_changed(path, self._serialize_json(payload))
        return path

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self.load()

    def _resolve_doc_for_query(self, query_name: str) -> dict[str, Any] | None:
        athlete_ids = self._name_index.get(canonical_name(query_name), set())
        if not athlete_ids:
            return None
        if len(athlete_ids) == 1:
            athlete_id = next(iter(athlete_ids))
            return self._docs.get(athlete_id)

        direct_matches: list[str] = []
        folded = " ".join(str(query_name).split()).casefold()
        for athlete_id in sorted(athlete_ids):
            doc = self._docs.get(athlete_id)
            if doc is None:
                continue
            identity = doc.get("identity")
            if not isinstance(identity, dict):
                continue
            aliases = identity.get("aliases", [])
            candidates = [identity.get("primary_name", "")] + (aliases if isinstance(aliases, list) else [])
            if any(str(candidate).strip().casefold() == folded for candidate in candidates):
                direct_matches.append(athlete_id)
        if len(direct_matches) == 1:
            return self._docs.get(direct_matches[0])
        return None

    def _resolve_doc_for_observations(
        self,
        *,
        input_name: str,
        observations: list[RepoProviderObservation],
    ) -> dict[str, Any] | None:
        exact_hits: list[str] = []
        for observation in observations:
            provider_uid = provider_uid_from_profile(observation.provider, observation.profile_url)
            if not provider_uid:
                continue
            athlete_id = self._provider_uid_index.get((observation.provider, provider_uid))
            if athlete_id and athlete_id not in exact_hits:
                exact_hits.append(athlete_id)

        if len(exact_hits) == 1:
            return self._docs.get(exact_hits[0])
        if exact_hits:
            strong_hits = [athlete_id for athlete_id in exact_hits if self._doc_matches_name(athlete_id, input_name)]
            if len(strong_hits) == 1:
                return self._docs.get(strong_hits[0])
            return self._docs.get(sorted(exact_hits)[0])

        possible_names = [input_name] + [obs.matched_name or "" for obs in observations]
        for candidate_name in possible_names:
            if not candidate_name:
                continue
            doc = self._resolve_doc_for_query(candidate_name)
            if doc is None:
                continue
            athlete_id = str(doc.get("identity", {}).get("athlete_id", "")).strip()
            if (
                athlete_id
                and self._doc_matches_name(athlete_id, input_name)
                and self._doc_can_accept_observations(athlete_id, observations)
            ):
                return doc
        return None

    def _doc_can_accept_observations(
        self,
        athlete_id: str,
        observations: list[RepoProviderObservation],
    ) -> bool:
        doc = self._docs.get(athlete_id)
        if doc is None:
            return False
        providers = doc.get("providers")
        if not isinstance(providers, dict):
            return True
        for observation in observations:
            existing = providers.get(observation.provider)
            if not isinstance(existing, dict):
                continue
            existing_uid = _text_or_none(existing.get("provider_uid"))
            incoming_uid = provider_uid_from_profile(observation.provider, observation.profile_url)
            if existing_uid and incoming_uid and existing_uid != incoming_uid:
                return False
        return True

    def _doc_matches_name(self, athlete_id: str, name: str) -> bool:
        doc = self._docs.get(athlete_id)
        if doc is None:
            return False
        identity = doc.get("identity")
        if not isinstance(identity, dict):
            return False
        primary_name = str(identity.get("primary_name", "")).strip()
        if primary_name and is_strong_person_name_match(name, primary_name):
            return True
        aliases = identity.get("aliases", [])
        if not isinstance(aliases, list):
            return False
        return any(is_strong_person_name_match(name, str(alias)) for alias in aliases if str(alias).strip())

    def _new_doc(self, *, primary_name: str, observed_at: datetime) -> dict[str, Any]:
        athlete_id = uuid4().hex
        return {
            "schema_version": ATHLETE_SCHEMA_VERSION,
            "identity": {
                "athlete_id": athlete_id,
                "primary_name": primary_name,
                "canonical_name": canonical_name(primary_name),
                "aliases": [primary_name],
            },
            "providers": {},
            "provenance": {
                "source_run_ids": [],
                "first_source_kind": "",
                "last_source_kind": "",
                "last_source_run_id": None,
            },
            "created_at": _isoformat(observed_at),
            "updated_at": _isoformat(observed_at),
        }

    def _should_persist_observation(self, observation: RepoProviderObservation) -> bool:
        if observation.status not in {"matched", "miss"}:
            return False
        if observation.status == "matched":
            return observation.persist and observation.score is not None
        return observation.persist

    def _snapshot_from_observation(
        self,
        *,
        observation: RepoProviderObservation,
        observed_at: datetime,
    ) -> dict[str, Any]:
        ttl_days = SUCCESS_TTL_DAYS if observation.status == "matched" else MISS_TTL_DAYS
        checked_at = (observation.checked_at or observed_at).astimezone(UTC)
        expires_at = checked_at + timedelta(days=ttl_days)
        return {
            "status": observation.status,
            "provider_uid": provider_uid_from_profile(observation.provider, observation.profile_url),
            "matched_name": observation.matched_name,
            "profile_url": observation.profile_url,
            "score": observation.score,
            "score_scale": observation.score_scale or provider_score_scale(observation.provider),
            "match_confidence": observation.match_confidence,
            "last_checked_at": _isoformat(checked_at),
            "expires_at": _isoformat(expires_at),
            "source_run_id": observation.source_run_id,
            "lookup_threshold": observation.lookup_threshold,
        }

    def _update_identity_aliases(
        self,
        doc: dict[str, Any],
        *,
        input_name: str,
        observations: list[RepoProviderObservation],
    ) -> bool:
        identity = doc.get("identity")
        if not isinstance(identity, dict):
            identity = {}
            doc["identity"] = identity

        athlete_id = str(identity.get("athlete_id", "")).strip() or uuid4().hex
        primary_name = str(identity.get("primary_name", "")).strip() or input_name
        aliases = identity.get("aliases", [])
        alias_values = aliases if isinstance(aliases, list) else []
        new_aliases = [primary_name, input_name] + [str(value) for value in alias_values]
        new_aliases.extend(
            observation.matched_name or ""
            for observation in observations
            if observation.status == "matched" and observation.persist
        )
        deduped = _dedupe_preserving_order(new_aliases)
        if not deduped:
            deduped = [primary_name]

        changed = (
            str(identity.get("athlete_id", "")).strip() != athlete_id
            or str(identity.get("primary_name", "")).strip() != primary_name
            or identity.get("canonical_name") != canonical_name(primary_name)
            or deduped != alias_values
        )
        identity["athlete_id"] = athlete_id
        identity["primary_name"] = primary_name
        identity["canonical_name"] = canonical_name(primary_name)
        identity["aliases"] = deduped
        return changed

    def _update_provenance(
        self,
        *,
        provenance: dict[str, Any],
        source_run_id: str | None,
        source_kind: str,
    ) -> bool:
        changed = False
        run_ids = provenance.get("source_run_ids", [])
        if not isinstance(run_ids, list):
            run_ids = []
        normalized_run_ids = [str(value).strip() for value in run_ids if str(value).strip()]
        if source_run_id and source_run_id not in normalized_run_ids:
            normalized_run_ids.append(source_run_id)
            normalized_run_ids = normalized_run_ids[-20:]
            changed = True

        first_source_kind = str(provenance.get("first_source_kind", "")).strip()
        if not first_source_kind:
            provenance["first_source_kind"] = source_kind
            changed = True

        if provenance.get("last_source_kind") != source_kind:
            provenance["last_source_kind"] = source_kind
            changed = True

        if provenance.get("last_source_run_id") != source_run_id:
            provenance["last_source_run_id"] = source_run_id
            changed = True

        if provenance.get("source_run_ids") != normalized_run_ids:
            provenance["source_run_ids"] = normalized_run_ids
            changed = True
        return changed

    def _serialize_json(self, payload: dict[str, Any]) -> str:
        return json.dumps(payload, indent=2, ensure_ascii=False) + "\n"

    def _normalize_doc(self, payload: dict[str, Any], *, fallback_athlete_id: str) -> dict[str, Any]:
        identity = payload.get("identity")
        if not isinstance(identity, dict):
            identity = {}
        athlete_id = str(identity.get("athlete_id", "")).strip() or fallback_athlete_id
        primary_name = str(identity.get("primary_name", "")).strip() or fallback_athlete_id
        aliases = identity.get("aliases", [])
        alias_values = aliases if isinstance(aliases, list) else []
        providers = payload.get("providers")
        if not isinstance(providers, dict):
            providers = {}
        normalized_providers: dict[str, dict[str, Any]] = {}
        for provider, snapshot in providers.items():
            if provider not in _PROVIDERS or not isinstance(snapshot, dict):
                continue
            normalized_providers[provider] = {
                "status": str(snapshot.get("status", "")).strip(),
                "provider_uid": _text_or_none(snapshot.get("provider_uid")),
                "matched_name": _text_or_none(snapshot.get("matched_name")),
                "profile_url": _text_or_none(snapshot.get("profile_url")),
                "score": _as_float(snapshot.get("score")),
                "score_scale": _text_or_none(snapshot.get("score_scale")) or provider_score_scale(provider),
                "match_confidence": _as_float(snapshot.get("match_confidence")),
                "last_checked_at": _isoformat(_normalize_dt(snapshot.get("last_checked_at")) or datetime.now(UTC)),
                "expires_at": _isoformat(_normalize_dt(snapshot.get("expires_at")) or datetime.now(UTC)),
                "source_run_id": _text_or_none(snapshot.get("source_run_id")),
                "lookup_threshold": _as_float(snapshot.get("lookup_threshold")),
            }
        provenance = payload.get("provenance")
        if not isinstance(provenance, dict):
            provenance = {}
        source_run_ids = provenance.get("source_run_ids", [])
        if not isinstance(source_run_ids, list):
            source_run_ids = []

        return {
            "schema_version": str(payload.get("schema_version", ATHLETE_SCHEMA_VERSION)),
            "identity": {
                "athlete_id": athlete_id,
                "primary_name": primary_name,
                "canonical_name": canonical_name(str(identity.get("canonical_name", "")) or primary_name),
                "aliases": _dedupe_preserving_order(
                    [primary_name] + [str(value) for value in alias_values]
                ),
            },
            "providers": normalized_providers,
            "provenance": {
                "source_run_ids": [str(value).strip() for value in source_run_ids if str(value).strip()],
                "first_source_kind": str(provenance.get("first_source_kind", "")).strip(),
                "last_source_kind": str(provenance.get("last_source_kind", "")).strip(),
                "last_source_run_id": _text_or_none(provenance.get("last_source_run_id")),
            },
            "created_at": _isoformat(_normalize_dt(payload.get("created_at")) or datetime.now(UTC)),
            "updated_at": _isoformat(_normalize_dt(payload.get("updated_at")) or datetime.now(UTC)),
        }

    def _athlete_path(self, athlete_id: str) -> Path:
        shard = athlete_id[:2] if len(athlete_id) >= 2 else "00"
        directory = self.root / "athletes" / shard
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{athlete_id}.json"

    def _doc_names(self, doc: dict[str, Any]) -> set[str]:
        identity = doc.get("identity")
        if not isinstance(identity, dict):
            return set()
        aliases = identity.get("aliases", [])
        names: list[str] = [str(identity.get("primary_name", ""))]
        if isinstance(aliases, list):
            names.extend(str(value) for value in aliases)
        return {canonical_name(name) for name in names if canonical_name(name)}

    def _doc_provider_keys(self, doc: dict[str, Any]) -> set[tuple[str, str]]:
        providers = doc.get("providers")
        if not isinstance(providers, dict):
            return set()
        keys: set[tuple[str, str]] = set()
        for provider, snapshot in providers.items():
            if not isinstance(snapshot, dict):
                continue
            provider_uid = _text_or_none(snapshot.get("provider_uid"))
            if provider_uid:
                keys.add((provider, provider_uid))
        return keys

    def _add_doc_indexes(self, doc: dict[str, Any]) -> None:
        identity = doc.get("identity")
        if not isinstance(identity, dict):
            return
        athlete_id = str(identity.get("athlete_id", "")).strip()
        if not athlete_id:
            return
        for name in self._doc_names(doc):
            self._name_index.setdefault(name, set()).add(athlete_id)
        for provider_key in self._doc_provider_keys(doc):
            self._provider_uid_index[provider_key] = athlete_id

    def _remove_index_entries(
        self,
        *,
        athlete_id: str,
        names: set[str],
        provider_keys: set[tuple[str, str]],
    ) -> None:
        for name in names:
            athlete_ids = self._name_index.get(name)
            if not athlete_ids:
                continue
            athlete_ids.discard(athlete_id)
            if not athlete_ids:
                self._name_index.pop(name, None)
        for provider_key in provider_keys:
            if self._provider_uid_index.get(provider_key) == athlete_id:
                self._provider_uid_index.pop(provider_key, None)

    @staticmethod
    def _write_if_changed(path: Path, content: str) -> bool:
        if path.exists() and path.read_text(encoding="utf-8") == content:
            return False
        path.write_text(content, encoding="utf-8")
        return True


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
