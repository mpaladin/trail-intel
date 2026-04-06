from __future__ import annotations

from base64 import b64encode
from datetime import UTC, datetime, timedelta
import json
import tempfile
import unittest
from unittest.mock import Mock, patch

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import requests

from trailintel.cache_store import LookupCacheStore
from trailintel.providers.itra import ItraClient, ItraLookupError, ItraMatch
from trailintel.providers.utmb import UtmbClient


def _encrypt_itra_response(payload: dict[str, object]) -> dict[str, str]:
    plaintext = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    pad_len = 16 - (len(plaintext) % 16)
    padded = plaintext + bytes([pad_len]) * pad_len
    key = b"0123456789ABCDEF0123456789ABCDEF"
    iv = b"ABCDEF0123456789"
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    encryptor = cipher.encryptor()
    encrypted = encryptor.update(padded) + encryptor.finalize()
    return {
        "response1": b64encode(encrypted).decode("ascii"),
        "response2": b64encode(iv).decode("ascii"),
        "response3": b64encode(key).decode("ascii"),
    }


class UtmbProviderSearchTests(unittest.TestCase):
    @patch("trailintel.providers.utmb.UtmbClient._fetch_runners")
    def test_same_name_candidates_keep_all_and_sort_by_score(self, mock_fetch_runners) -> None:
        def _fake_fetch(search_text: str, *, limit: int):
            if search_text == "Aurélien Dupont":
                return [
                    {"fullname": "Aurélien Dupont", "ip": 750, "uri": "/runners/1"},
                    {"fullname": "Aurélien Dupont", "ip": 760, "uri": "/runners/2"},
                    {"fullname": "Aurelien Durand", "ip": 900, "uri": "/runners/3"},
                ]
            return [
                {"fullname": "Aurelien Dupont", "ip": 780, "uri": "/runners/4"},
                {"fullname": "Aurélien Dupont", "ip": 760, "uri": "/runners/2"},
            ]

        mock_fetch_runners.side_effect = _fake_fetch
        client = UtmbClient(timeout=5)
        candidates = client.search_same_name_candidates("Aurélien Dupont")

        self.assertEqual(len(candidates), 3)
        self.assertEqual([c.utmb_index for c in candidates], [780.0, 760.0, 750.0])
        self.assertEqual(client.search("Aurélien Dupont").utmb_index, 780.0)

    @patch("trailintel.providers.utmb.UtmbClient._fetch_runners")
    def test_rejects_wrong_same_first_name(self, mock_fetch_runners) -> None:
        mock_fetch_runners.return_value = [
            {"fullname": "Marianne Hogan", "ip": 780, "uri": "/runners/hogan"},
        ]
        client = UtmbClient(timeout=5)
        self.assertEqual(client.search_same_name_candidates("Marianne Coquard"), [])
        self.assertIsNone(client.search("Marianne Coquard"))

    @patch("trailintel.providers.utmb.UtmbClient._fetch_runners")
    def test_cache_hit_avoids_second_network_call(self, mock_fetch_runners) -> None:
        mock_fetch_runners.return_value = [
            {"fullname": "John Doe", "ip": 711, "uri": "123.john.doe"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            client = UtmbClient(timeout=5, cache_store=store, use_cache=True)
            first = client.search_same_name_candidates("John Doe")
            self.assertEqual(len(first), 1)
            self.assertEqual(mock_fetch_runners.call_count, 1)

            second = client.search_same_name_candidates("John Doe")
            self.assertEqual(len(second), 1)
            self.assertEqual(mock_fetch_runners.call_count, 1)
            self.assertTrue(client.last_lookup_used_cache)

            refresh_client = UtmbClient(
                timeout=5,
                cache_store=store,
                use_cache=True,
                force_refresh=True,
            )
            refresh_client.search_same_name_candidates("John Doe")
            self.assertEqual(mock_fetch_runners.call_count, 2)
            store.close()

    @patch("trailintel.providers.utmb.UtmbClient._fetch_runners")
    def test_miss_is_cached(self, mock_fetch_runners) -> None:
        mock_fetch_runners.return_value = []
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            client = UtmbClient(timeout=5, cache_store=store, use_cache=True)
            self.assertEqual(client.search_same_name_candidates("Nobody Runner"), [])
            self.assertEqual(mock_fetch_runners.call_count, 1)
            self.assertEqual(client.search_same_name_candidates("Nobody Runner"), [])
            self.assertEqual(mock_fetch_runners.call_count, 1)
            store.close()

    @patch("trailintel.providers.utmb.UtmbClient._fetch_runners")
    def test_stale_fallback_is_used_when_live_errors(self, mock_fetch_runners) -> None:
        mock_fetch_runners.side_effect = requests.RequestException("network down")
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            store.put_lookup(
                provider="utmb",
                query_name="John Doe",
                auth_scope="public",
                status="success",
                payload_json='[{"matched_name":"John Doe","utmb_index":750,"profile_url":"https://utmb.world/runner/123.john.doe","match_score":1.0}]',
                fetched_at=datetime.now(UTC) - timedelta(days=90),
            )
            client = UtmbClient(timeout=5, cache_store=store, use_cache=True)
            candidates = client.search_same_name_candidates("John Doe")
            self.assertEqual(len(candidates), 1)
            self.assertTrue(client.last_lookup_stale_fallback)
            self.assertEqual(candidates[0].source, "stale_cache")
            store.close()


class ItraProviderSearchTests(unittest.TestCase):
    @patch("trailintel.providers.itra.ItraClient._bootstrap")
    @patch("trailintel.providers.itra.requests.Session.post")
    def test_encrypted_find_payload_decoded_with_uppercase_fields(
        self,
        mock_post,
        mock_bootstrap,
    ) -> None:
        mock_bootstrap.return_value = None
        encrypted_payload = _encrypt_itra_response(
            {
                "ResultCount": 1,
                "EchoToken": 0.1,
                "Results": [
                    {
                        "RunnerId": 921767,
                        "FirstName": "Massimo",
                        "LastName": "PALADIN",
                        "Pi": 778,
                    }
                ],
            }
        )

        response = Mock()
        response.status_code = 200
        response.text = ""
        response.json.return_value = encrypted_payload
        response.raise_for_status.return_value = None
        mock_post.return_value = response

        client = ItraClient(timeout=5)
        client._csrf_token = "csrf-token"
        matches = client.search_same_name_candidates("Massimo Paladin")

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].matched_name, "Massimo PALADIN")
        self.assertEqual(matches[0].itra_score, 778.0)
        self.assertEqual(
            matches[0].profile_url,
            "https://itra.run/RunnerSpace/PALADIN.Massimo/921767",
        )

    @patch("trailintel.providers.itra.ItraClient._search_candidates")
    def test_cookie_mode_retries_anonymous_on_auth_failure(self, mock_search_candidates) -> None:
        mock_search_candidates.side_effect = [
            ItraLookupError("ITRA lookup failed: /api/runner/find HTTP 400"),
            [
                ItraMatch(
                    query_name="John Doe",
                    matched_name="John Doe",
                    itra_score=741.0,
                    profile_url="https://itra.run/RunnerSpace/Doe.John/1",
                    match_score=1.0,
                    raw={},
                )
            ],
        ]

        client = ItraClient(timeout=5, cookie="session=broken")
        matches = client.search_same_name_candidates("John Doe")

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].itra_score, 741.0)
        self.assertTrue(client.last_lookup_used_cookie_fallback)
        self.assertEqual(mock_search_candidates.call_count, 2)

    @patch("trailintel.providers.itra.ItraClient._search_candidates")
    def test_no_cookie_mode_does_not_retry_anonymous(self, mock_search_candidates) -> None:
        mock_search_candidates.side_effect = ItraLookupError(
            "ITRA lookup failed: /api/runner/findByName HTTP 403"
        )

        client = ItraClient(timeout=5, cookie=None)
        with self.assertRaises(ItraLookupError):
            client.search_same_name_candidates("John Doe")

        self.assertFalse(client.last_lookup_used_cookie_fallback)
        self.assertEqual(mock_search_candidates.call_count, 1)

    @patch("trailintel.providers.itra.ItraClient._bootstrap")
    @patch("trailintel.providers.itra.ItraClient._post_search")
    def test_cloudfront_403_surfaces_clear_error_message(
        self,
        mock_post_search,
        mock_bootstrap,
    ) -> None:
        mock_bootstrap.return_value = None
        response = requests.Response()
        response.status_code = 403
        response._content = (
            b"<html><body>Request blocked. Generated by cloudfront (CloudFront)</body></html>"
        )
        http_error = requests.HTTPError("403 Forbidden", response=response)
        mock_post_search.side_effect = http_error

        client = ItraClient(timeout=5)
        with self.assertRaisesRegex(ItraLookupError, "CloudFront"):
            client.search("John Doe")

    @patch("trailintel.providers.itra.ItraClient._bootstrap")
    @patch("trailintel.providers.itra.ItraClient._post_search")
    def test_same_name_candidates_keep_all_and_sort_by_score(self, mock_post_search, mock_bootstrap) -> None:
        mock_bootstrap.return_value = None

        def _fake_post(path, *, data, headers):
            query_name = data.get("name", "")
            if path.endswith("/findByName"):
                return []
            if query_name == "Aurélie Martin":
                return [
                    {"firstName": "Aurélie", "lastName": "Martin", "runnerId": 1, "pi": 700},
                    {"firstName": "Aurélie", "lastName": "Martin", "runnerId": 2, "pi": 720},
                    {"firstName": "Aurélie", "lastName": "Durand", "runnerId": 3, "pi": 900},
                ]
            if query_name == "Aurelie Martin":
                return [
                    {"firstName": "Aurelie", "lastName": "Martin", "runnerId": 4, "pi": 730},
                ]
            return []

        mock_post_search.side_effect = _fake_post
        client = ItraClient(timeout=5)
        candidates = client.search_same_name_candidates("Aurélie Martin")

        self.assertEqual(len(candidates), 3)
        self.assertEqual([c.itra_score for c in candidates], [730.0, 720.0, 700.0])
        best = client.search("Aurélie Martin")
        self.assertIsNotNone(best)
        self.assertEqual(best.itra_score, 730.0)

    @patch("trailintel.providers.itra.ItraClient._bootstrap")
    @patch("trailintel.providers.itra.ItraClient._post_search")
    def test_rejects_wrong_same_first_name(self, mock_post_search, mock_bootstrap) -> None:
        mock_bootstrap.return_value = None

        def _fake_post(path, *, data, headers):
            if path.endswith("/findByName"):
                return []
            return [
                {"firstName": "Marianne", "lastName": "Hogan", "runnerId": 1, "pi": 900},
            ]

        mock_post_search.side_effect = _fake_post
        client = ItraClient(timeout=5)
        self.assertEqual(client.search_same_name_candidates("Marianne Coquard"), [])
        self.assertIsNone(client.search("Marianne Coquard"))

    @patch("trailintel.providers.itra.ItraClient._bootstrap")
    @patch("trailintel.providers.itra.ItraClient._post_search")
    def test_cache_hit_avoids_second_network_call(self, mock_post_search, mock_bootstrap) -> None:
        mock_bootstrap.return_value = None

        def _fake_post(path, *, data, headers):
            if path.endswith("/findByName"):
                return []
            return [{"firstName": "John", "lastName": "Doe", "runnerId": 1, "pi": 740}]

        mock_post_search.side_effect = _fake_post
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            client = ItraClient(timeout=5, cache_store=store, use_cache=True)
            first = client.search_same_name_candidates("John Doe")
            self.assertEqual(len(first), 1)
            initial_calls = mock_post_search.call_count

            second = client.search_same_name_candidates("John Doe")
            self.assertEqual(len(second), 1)
            self.assertEqual(mock_post_search.call_count, initial_calls)
            self.assertTrue(client.last_lookup_used_cache)

            refresh_client = ItraClient(
                timeout=5,
                cache_store=store,
                use_cache=True,
                force_refresh=True,
            )
            refresh_client.search_same_name_candidates("John Doe")
            self.assertGreater(mock_post_search.call_count, initial_calls)
            store.close()

    @patch("trailintel.providers.itra.ItraClient._bootstrap")
    @patch("trailintel.providers.itra.ItraClient._post_search")
    def test_miss_is_cached(self, mock_post_search, mock_bootstrap) -> None:
        mock_bootstrap.return_value = None
        mock_post_search.return_value = []
        with tempfile.TemporaryDirectory() as tmp:
            store = LookupCacheStore(f"{tmp}/cache.duckdb")
            client = ItraClient(timeout=5, cache_store=store, use_cache=True)
            self.assertEqual(client.search_same_name_candidates("Nobody Runner"), [])
            initial_calls = mock_post_search.call_count
            self.assertEqual(client.search_same_name_candidates("Nobody Runner"), [])
            self.assertEqual(mock_post_search.call_count, initial_calls)
            store.close()


if __name__ == "__main__":
    unittest.main()
