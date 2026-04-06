from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb

from trailintel.cache_store import RACE_HISTORY_MAX_RUNS, LookupCacheStore, default_cache_db_path


class CacheStoreTests(unittest.TestCase):
    def test_default_cache_db_path_uses_config_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = f"{tmp}/config.toml"
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write("[cache]\n")
                handle.write(f'db_path = "{tmp}/from-config.duckdb"\n')

            with patch.dict(
                os.environ,
                {
                    "TRAILINTEL_CONFIG_FILE": config_path,
                },
                clear=False,
            ):
                os.environ.pop("TRAILINTEL_CACHE_DB", None)
                resolved = default_cache_db_path()

            self.assertEqual(str(resolved), f"{tmp}/from-config.duckdb")

    def test_default_cache_db_path_new_env_overrides_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = f"{tmp}/config.toml"
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write("[cache]\n")
                handle.write(f'db_path = "{tmp}/from-config.duckdb"\n')

            with patch.dict(
                os.environ,
                {
                    "TRAILINTEL_CONFIG_FILE": config_path,
                    "TRAILINTEL_CACHE_DB": f"{tmp}/from-env.duckdb",
                },
                clear=False,
            ):
                resolved = default_cache_db_path()

            self.assertEqual(str(resolved), f"{tmp}/from-env.duckdb")

    def test_default_cache_db_path_falls_back_to_default_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            default_path = home / ".cache" / "trailintel" / "trailintel_cache.duckdb"

            with patch.dict(
                os.environ,
                {
                    "HOME": str(home),
                },
                clear=False,
            ):
                os.environ.pop("TRAILINTEL_CACHE_DB", None)
                os.environ.pop("TRAILINTEL_CONFIG_FILE", None)
                resolved = default_cache_db_path()

            self.assertEqual(resolved, default_path)

    def test_schema_is_created(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = f"{tmp}/cache.duckdb"
            store = LookupCacheStore(db_path)
            store.close()

            conn = duckdb.connect(db_path)
            exists = conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name IN ('athlete_lookup_cache', 'saved_races', 'race_run_history', 'app_settings')
                """
            ).fetchone()[0]
            conn.close()
            self.assertEqual(exists, 4)

    def test_success_ttl_is_60_days(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            fetched_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
            entry = store.put_lookup(
                provider="utmb",
                query_name="Kilian Jornet",
                auth_scope="public",
                status="success",
                payload_json='[{"matched_name":"Kilian Jornet","utmb_index":930}]',
                fetched_at=fetched_at,
            )
            store.close()
            self.assertEqual((entry.expires_at - fetched_at).days, 60)

    def test_miss_ttl_is_7_days(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            fetched_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
            entry = store.put_lookup(
                provider="itra",
                query_name="Unknown Runner",
                auth_scope="public",
                status="miss",
                payload_json="[]",
                fetched_at=fetched_at,
            )
            store.close()
            self.assertEqual((entry.expires_at - fetched_at).days, 7)

    def test_get_lookup_hit_and_miss(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            self.assertIsNone(
                store.get_lookup(
                    provider="utmb",
                    query_name="A Name",
                    auth_scope="public",
                )
            )
            store.put_lookup(
                provider="utmb",
                query_name="A Name",
                auth_scope="public",
                status="success",
                payload_json='[{"matched_name":"A Name","utmb_index":700}]',
            )
            cached = store.get_lookup(
                provider="utmb",
                query_name="A Name",
                auth_scope="public",
            )
            store.close()
            self.assertIsNotNone(cached)
            assert cached is not None
            self.assertEqual(cached.status, "success")
            self.assertFalse(cached.is_stale)

    def test_stale_entry_is_returned_with_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            stale_fetched = datetime.now(UTC) - timedelta(days=75)
            store.put_lookup(
                provider="itra",
                query_name="Runner Old",
                auth_scope="auth",
                status="success",
                payload_json='[{"matched_name":"Runner Old","itra_score":750}]',
                fetched_at=stale_fetched,
            )
            cached = store.get_lookup(
                provider="itra",
                query_name="Runner Old",
                auth_scope="auth",
            )
            store.close()
            self.assertIsNotNone(cached)
            assert cached is not None
            self.assertTrue(cached.is_stale)
            self.assertIn("Runner Old", cached.payload_json)

    def test_seed_default_races_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            defaults = [
                ("Race A", "https://example.com/a", "40 km"),
                ("Race B", "https://example.com/b", "20 km"),
            ]
            first = store.seed_default_races(defaults)
            second = store.seed_default_races(defaults)
            races = store.list_saved_races()
            store.close()

            self.assertEqual(first, 2)
            self.assertEqual(second, 0)
            self.assertEqual(len(races), 2)

    def test_upsert_saved_race_updates_existing_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            first = store.upsert_saved_race(
                race_label="Old Label",
                race_url="https://example.com/race",
                competition_name="42 km",
            )
            second = store.upsert_saved_race(
                race_label="New Label",
                race_url="https://example.com/race",
                competition_name="42 km",
            )
            races = store.list_saved_races()
            store.close()

            self.assertEqual(first.race_key, second.race_key)
            self.assertEqual(len(races), 1)
            self.assertEqual(races[0].race_label, "New Label")

    def test_delete_saved_race_removes_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            race = store.upsert_saved_race(
                race_label="Race",
                race_url="https://example.com/race",
                competition_name="42 km",
            )
            store.append_race_run(
                race_key=race.race_key,
                payload_json=json.dumps({"title": "Race"}),
                participants_count=10,
                rows_evaluated=10,
                qualified_count=2,
                strategy="participant-first",
                same_name_mode="highest",
            )
            deleted = store.delete_saved_race(race.race_key)
            runs = store.list_race_runs(race.race_key, limit=100)
            store.close()

            self.assertTrue(deleted)
            self.assertEqual(runs, [])

    def test_append_race_run_retention_keeps_newest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            race = store.upsert_saved_race(
                race_label="Race",
                race_url="https://example.com/race",
                competition_name="42 km",
            )
            base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
            total = RACE_HISTORY_MAX_RUNS + 5
            for idx in range(total):
                store.append_race_run(
                    race_key=race.race_key,
                    payload_json=json.dumps({"run": idx}),
                    participants_count=idx,
                    rows_evaluated=idx,
                    qualified_count=idx,
                    strategy="participant-first",
                    same_name_mode="highest",
                    run_at=base + timedelta(minutes=idx),
                    max_runs=RACE_HISTORY_MAX_RUNS,
                )
            runs = store.list_race_runs(race.race_key, limit=100)
            store.close()

            self.assertEqual(len(runs), RACE_HISTORY_MAX_RUNS)
            self.assertEqual(runs[0].participants_count, total - 1)
            self.assertEqual(runs[-1].participants_count, total - RACE_HISTORY_MAX_RUNS)

    def test_get_race_run_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            race = store.upsert_saved_race(
                race_label="Race",
                race_url="https://example.com/race",
                competition_name="42 km",
            )
            expected_snapshot = {"title": "Race", "rows": [{"Athlete": "A"}]}
            saved = store.append_race_run(
                race_key=race.race_key,
                payload_json=json.dumps(expected_snapshot, ensure_ascii=False),
                participants_count=12,
                rows_evaluated=20,
                qualified_count=3,
                strategy="participant-first",
                same_name_mode="keep_all",
            )
            loaded = store.get_race_run(saved.run_id)
            store.close()

            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(json.loads(loaded.payload_json), expected_snapshot)
            self.assertEqual(loaded.strategy, "participant-first")
            self.assertEqual(loaded.same_name_mode, "keep_all")

    def test_app_setting_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            self.assertIsNone(store.get_setting("itra_cookie"))
            store.put_setting(setting_key="itra_cookie", setting_value="session=abc")
            self.assertEqual(store.get_setting("itra_cookie"), "session=abc")
            store.put_setting(setting_key="itra_cookie", setting_value="session=def")
            self.assertEqual(store.get_setting("itra_cookie"), "session=def")
            store.close()


if __name__ == "__main__":
    unittest.main()
