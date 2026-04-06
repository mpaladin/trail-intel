from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import os
from pathlib import Path
import tomllib
from uuid import uuid4

import duckdb

from trailintel.matching import canonical_name

SUCCESS_TTL_DAYS = 60
MISS_TTL_DAYS = 7
RACE_HISTORY_MAX_RUNS = 20


def default_config_file_path() -> Path:
    configured = os.getenv("TRAILINTEL_CONFIG_FILE")
    if configured:
        return Path(configured).expanduser()
    return Path("~/.config/trailintel/config.toml").expanduser()


def _cache_db_path_from_config(config_path: Path | None = None) -> Path | None:
    path = (config_path or default_config_file_path()).expanduser()
    if not path.exists():
        return None
    try:
        parsed = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None

    # Preferred format:
    # [cache]
    # db_path = "/path/to/trailintel_cache.duckdb"
    cache = parsed.get("cache")
    if isinstance(cache, dict):
        configured = cache.get("db_path")
        if isinstance(configured, str) and configured.strip():
            return Path(configured.strip()).expanduser()
    return None


def default_cache_db_path() -> Path:
    configured = os.getenv("TRAILINTEL_CACHE_DB")
    if configured:
        return Path(configured).expanduser()
    from_config = _cache_db_path_from_config()
    if from_config:
        return from_config
    return Path("~/.cache/trailintel/trailintel_cache.duckdb").expanduser()


@dataclass(slots=True)
class LookupCacheEntry:
    provider: str
    query_key: str
    auth_scope: str
    status: str
    payload_json: str
    fetched_at: datetime
    expires_at: datetime
    updated_at: datetime
    is_stale: bool


@dataclass(slots=True)
class SavedRaceEntry:
    race_key: str
    race_label: str
    race_url: str
    competition_name: str
    created_at: datetime
    updated_at: datetime
    last_run_at: datetime | None


@dataclass(slots=True)
class RaceRunHistoryEntry:
    run_id: str
    race_key: str
    run_at: datetime
    payload_json: str
    participants_count: int
    rows_evaluated: int
    qualified_count: int
    strategy: str
    same_name_mode: str


class LookupCacheStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self.db_path))
        self.ensure_schema()

    def ensure_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS athlete_lookup_cache (
                provider TEXT NOT NULL,
                query_key TEXT NOT NULL,
                auth_scope TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                fetched_at TIMESTAMP NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (provider, query_key, auth_scope)
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS saved_races (
                race_key TEXT PRIMARY KEY,
                race_label TEXT NOT NULL,
                race_url TEXT NOT NULL,
                competition_name TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                last_run_at TIMESTAMP
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS race_run_history (
                run_id TEXT PRIMARY KEY,
                race_key TEXT NOT NULL,
                run_at TIMESTAMP NOT NULL,
                payload_json TEXT NOT NULL,
                participants_count INTEGER NOT NULL,
                rows_evaluated INTEGER NOT NULL,
                qualified_count INTEGER NOT NULL,
                strategy TEXT NOT NULL,
                same_name_mode TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_race_run_history_race_time
            ON race_run_history(race_key, run_at)
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                setting_key TEXT PRIMARY KEY,
                setting_value TEXT NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
            """
        )

    @staticmethod
    def build_race_key(*, race_url: str, competition_name: str) -> str:
        normalized_url = " ".join(race_url.strip().lower().split())
        normalized_competition = (
            canonical_name(competition_name.strip()).replace(" ", "")
            if competition_name
            else ""
        )
        token = f"{normalized_url}\n{normalized_competition}"
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def get_lookup(
        self,
        *,
        provider: str,
        query_name: str,
        auth_scope: str,
    ) -> LookupCacheEntry | None:
        query_key = canonical_name(query_name)
        row = self._conn.execute(
            """
            SELECT
                provider,
                query_key,
                auth_scope,
                status,
                payload_json,
                fetched_at,
                expires_at,
                updated_at
            FROM athlete_lookup_cache
            WHERE provider = ? AND query_key = ? AND auth_scope = ?
            LIMIT 1
            """,
            [provider, query_key, auth_scope],
        ).fetchone()
        if not row:
            return None

        now = datetime.now(UTC)
        fetched_at = self._normalize_dt(row[5])
        expires_at = self._normalize_dt(row[6])
        updated_at = self._normalize_dt(row[7])
        return LookupCacheEntry(
            provider=row[0],
            query_key=row[1],
            auth_scope=row[2],
            status=row[3],
            payload_json=row[4] or "",
            fetched_at=fetched_at,
            expires_at=expires_at,
            updated_at=updated_at,
            is_stale=expires_at <= now,
        )

    def put_lookup(
        self,
        *,
        provider: str,
        query_name: str,
        auth_scope: str,
        status: str,
        payload_json: str,
        fetched_at: datetime | None = None,
    ) -> LookupCacheEntry:
        if status not in {"success", "miss"}:
            raise ValueError(f"Unsupported cache status: {status}")

        now = self._normalize_dt(fetched_at or datetime.now(UTC))
        ttl_days = SUCCESS_TTL_DAYS if status == "success" else MISS_TTL_DAYS
        expires_at = now + timedelta(days=ttl_days)
        updated_at = datetime.now(UTC)
        query_key = canonical_name(query_name)

        self._conn.execute(
            "DELETE FROM athlete_lookup_cache WHERE provider = ? AND query_key = ? AND auth_scope = ?",
            [provider, query_key, auth_scope],
        )
        self._conn.execute(
            """
            INSERT INTO athlete_lookup_cache (
                provider,
                query_key,
                auth_scope,
                status,
                payload_json,
                fetched_at,
                expires_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                provider,
                query_key,
                auth_scope,
                status,
                payload_json,
                now,
                expires_at,
                updated_at,
            ],
        )

        return LookupCacheEntry(
            provider=provider,
            query_key=query_key,
            auth_scope=auth_scope,
            status=status,
            payload_json=payload_json,
            fetched_at=now,
            expires_at=expires_at,
            updated_at=updated_at,
            is_stale=False,
        )

    def close(self) -> None:
        self._conn.close()

    def get_setting(self, setting_key: str) -> str | None:
        key = setting_key.strip()
        if not key:
            raise ValueError("setting_key must not be empty")
        row = self._conn.execute(
            """
            SELECT setting_value
            FROM app_settings
            WHERE setting_key = ?
            LIMIT 1
            """,
            [key],
        ).fetchone()
        if not row:
            return None
        value = row[0]
        return value if isinstance(value, str) else str(value)

    def put_setting(self, *, setting_key: str, setting_value: str) -> None:
        key = setting_key.strip()
        if not key:
            raise ValueError("setting_key must not be empty")
        self._conn.execute("DELETE FROM app_settings WHERE setting_key = ?", [key])
        self._conn.execute(
            """
            INSERT INTO app_settings (
                setting_key,
                setting_value,
                updated_at
            )
            VALUES (?, ?, ?)
            """,
            [key, setting_value, datetime.now(UTC)],
        )

    def upsert_saved_race(
        self,
        *,
        race_label: str,
        race_url: str,
        competition_name: str,
        last_run_at: datetime | None = None,
    ) -> SavedRaceEntry:
        normalized_url = " ".join(race_url.strip().split())
        if not normalized_url:
            raise ValueError("race_url is required to save a race preset")
        normalized_competition = " ".join(competition_name.strip().split())
        normalized_label = " ".join(race_label.strip().split()) or normalized_competition or normalized_url

        race_key = self.build_race_key(
            race_url=normalized_url,
            competition_name=normalized_competition,
        )
        now = datetime.now(UTC)
        existing = self.get_saved_race(race_key)
        created_at = existing.created_at if existing else now
        effective_last_run = (
            self._normalize_dt(last_run_at)
            if last_run_at is not None
            else (existing.last_run_at if existing else None)
        )

        self._conn.execute("DELETE FROM saved_races WHERE race_key = ?", [race_key])
        self._conn.execute(
            """
            INSERT INTO saved_races (
                race_key,
                race_label,
                race_url,
                competition_name,
                created_at,
                updated_at,
                last_run_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                race_key,
                normalized_label,
                normalized_url,
                normalized_competition,
                created_at,
                now,
                effective_last_run,
            ],
        )
        return SavedRaceEntry(
            race_key=race_key,
            race_label=normalized_label,
            race_url=normalized_url,
            competition_name=normalized_competition,
            created_at=created_at,
            updated_at=now,
            last_run_at=effective_last_run,
        )

    def list_saved_races(self) -> list[SavedRaceEntry]:
        rows = self._conn.execute(
            """
            SELECT
                race_key,
                race_label,
                race_url,
                competition_name,
                created_at,
                updated_at,
                last_run_at
            FROM saved_races
            ORDER BY COALESCE(last_run_at, updated_at) DESC, race_label ASC
            """
        ).fetchall()
        return [
            SavedRaceEntry(
                race_key=row[0],
                race_label=row[1],
                race_url=row[2],
                competition_name=row[3],
                created_at=self._normalize_dt(row[4]),
                updated_at=self._normalize_dt(row[5]),
                last_run_at=self._normalize_optional_dt(row[6]),
            )
            for row in rows
        ]

    def get_saved_race(self, race_key: str) -> SavedRaceEntry | None:
        row = self._conn.execute(
            """
            SELECT
                race_key,
                race_label,
                race_url,
                competition_name,
                created_at,
                updated_at,
                last_run_at
            FROM saved_races
            WHERE race_key = ?
            LIMIT 1
            """,
            [race_key],
        ).fetchone()
        if not row:
            return None
        return SavedRaceEntry(
            race_key=row[0],
            race_label=row[1],
            race_url=row[2],
            competition_name=row[3],
            created_at=self._normalize_dt(row[4]),
            updated_at=self._normalize_dt(row[5]),
            last_run_at=self._normalize_optional_dt(row[6]),
        )

    def delete_saved_race(self, race_key: str) -> bool:
        existing = self.get_saved_race(race_key)
        if existing is None:
            return False
        self._conn.execute("DELETE FROM race_run_history WHERE race_key = ?", [race_key])
        self._conn.execute("DELETE FROM saved_races WHERE race_key = ?", [race_key])
        return True

    def seed_default_races(self, races: list[tuple[str, str, str]]) -> int:
        inserted = 0
        for race_label, race_url, competition_name in races:
            race_key = self.build_race_key(race_url=race_url, competition_name=competition_name)
            if self.get_saved_race(race_key) is not None:
                continue
            self.upsert_saved_race(
                race_label=race_label,
                race_url=race_url,
                competition_name=competition_name,
            )
            inserted += 1
        return inserted

    def append_race_run(
        self,
        *,
        race_key: str,
        payload_json: str,
        participants_count: int,
        rows_evaluated: int,
        qualified_count: int,
        strategy: str,
        same_name_mode: str,
        run_at: datetime | None = None,
        max_runs: int = RACE_HISTORY_MAX_RUNS,
    ) -> RaceRunHistoryEntry:
        race = self.get_saved_race(race_key)
        if race is None:
            raise ValueError(f"unknown race_key: {race_key}")

        effective_run_at = self._normalize_dt(run_at or datetime.now(UTC))
        run_id = str(uuid4())
        self._conn.execute(
            """
            INSERT INTO race_run_history (
                run_id,
                race_key,
                run_at,
                payload_json,
                participants_count,
                rows_evaluated,
                qualified_count,
                strategy,
                same_name_mode
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                race_key,
                effective_run_at,
                payload_json,
                int(participants_count),
                int(rows_evaluated),
                int(qualified_count),
                strategy,
                same_name_mode,
            ],
        )
        self._conn.execute(
            """
            UPDATE saved_races
            SET last_run_at = ?, updated_at = ?
            WHERE race_key = ?
            """,
            [effective_run_at, datetime.now(UTC), race_key],
        )

        if max_runs > 0:
            self._conn.execute(
                """
                DELETE FROM race_run_history
                WHERE run_id IN (
                    SELECT run_id
                    FROM race_run_history
                    WHERE race_key = ?
                    ORDER BY run_at DESC, run_id DESC
                    OFFSET ?
                )
                """,
                [race_key, max_runs],
            )

        return RaceRunHistoryEntry(
            run_id=run_id,
            race_key=race_key,
            run_at=effective_run_at,
            payload_json=payload_json,
            participants_count=int(participants_count),
            rows_evaluated=int(rows_evaluated),
            qualified_count=int(qualified_count),
            strategy=strategy,
            same_name_mode=same_name_mode,
        )

    def list_race_runs(self, race_key: str, *, limit: int = RACE_HISTORY_MAX_RUNS) -> list[RaceRunHistoryEntry]:
        effective_limit = max(int(limit), 1)
        rows = self._conn.execute(
            """
            SELECT
                run_id,
                race_key,
                run_at,
                payload_json,
                participants_count,
                rows_evaluated,
                qualified_count,
                strategy,
                same_name_mode
            FROM race_run_history
            WHERE race_key = ?
            ORDER BY run_at DESC, run_id DESC
            LIMIT ?
            """,
            [race_key, effective_limit],
        ).fetchall()
        return [
            RaceRunHistoryEntry(
                run_id=row[0],
                race_key=row[1],
                run_at=self._normalize_dt(row[2]),
                payload_json=row[3] or "",
                participants_count=int(row[4]),
                rows_evaluated=int(row[5]),
                qualified_count=int(row[6]),
                strategy=row[7],
                same_name_mode=row[8],
            )
            for row in rows
        ]

    def get_race_run(self, run_id: str) -> RaceRunHistoryEntry | None:
        row = self._conn.execute(
            """
            SELECT
                run_id,
                race_key,
                run_at,
                payload_json,
                participants_count,
                rows_evaluated,
                qualified_count,
                strategy,
                same_name_mode
            FROM race_run_history
            WHERE run_id = ?
            LIMIT 1
            """,
            [run_id],
        ).fetchone()
        if not row:
            return None
        return RaceRunHistoryEntry(
            run_id=row[0],
            race_key=row[1],
            run_at=self._normalize_dt(row[2]),
            payload_json=row[3] or "",
            participants_count=int(row[4]),
            rows_evaluated=int(row[5]),
            qualified_count=int(row[6]),
            strategy=row[7],
            same_name_mode=row[8],
        )

    @staticmethod
    def _normalize_dt(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @staticmethod
    def _normalize_optional_dt(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return LookupCacheStore._normalize_dt(value)
