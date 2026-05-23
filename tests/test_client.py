"""Tests for WHOOP API client."""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from whoop_mcp.client import WhoopClient, WhoopAuthError, WhoopAPIError, TOKEN_LIFETIME_MINUTES, find_env_file


# --- Fixtures ---

@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Set required env vars for all client tests."""
    monkeypatch.setenv("WHOOP_ACCESS_TOKEN", "test-access-token")
    monkeypatch.setenv("WHOOP_REFRESH_TOKEN", "test-refresh-token")
    monkeypatch.setenv("WHOOP_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("WHOOP_CLIENT_SECRET", "test-client-secret")


@pytest.fixture(autouse=True)
def mock_disk_dotenv(monkeypatch):
    """Default disk-read to empty so tests don't pick up the real local .env.

    Individual tests that exercise the cross-process token-rotation path
    override this with patch("whoop_mcp.client.dotenv_values", ...).
    """
    monkeypatch.setattr("whoop_mcp.client.dotenv_values", lambda _path: {})


@pytest.fixture(autouse=True)
def reset_last_refresh():
    """Reset the module-level _last_token_refresh between tests.

    Without this, tests that exercise _refresh_access_token leave the
    global set to datetime.now(), so subsequent tests see
    _token_needs_refresh() == False and early-return from the refresh
    path — making "raises if no refresh token" tests silently pass.
    """
    import whoop_mcp.client as client_mod
    client_mod._last_token_refresh = None
    yield
    client_mod._last_token_refresh = None


@pytest.fixture
def client():
    return WhoopClient()


# --- Init ---

class TestClientInit:
    def test_loads_tokens_from_env(self, client):
        assert client.access_token == "test-access-token"
        assert client.refresh_token == "test-refresh-token"
        assert client.client_id == "test-client-id"
        assert client.client_secret == "test-client-secret"

    def test_raises_without_access_token(self, monkeypatch):
        monkeypatch.delenv("WHOOP_ACCESS_TOKEN")
        with pytest.raises(WhoopAuthError, match="No access token"):
            WhoopClient()


# --- Token refresh logic ---

class TestTokenRefresh:
    def test_needs_refresh_when_never_refreshed(self, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = None
        assert client._token_needs_refresh() is True

    def test_no_refresh_when_recently_refreshed(self, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()
        assert client._token_needs_refresh() is False

    def test_needs_refresh_after_timeout(self, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now() - timedelta(minutes=TOKEN_LIFETIME_MINUTES + 1)
        assert client._token_needs_refresh() is True

    @patch("whoop_mcp.client.set_key")
    def test_refresh_updates_tokens(self, mock_set_key, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
        }

        mock_http_client = AsyncMock()
        mock_http_client.post.return_value = mock_response
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http_client):
            asyncio.run(client._refresh_access_token())

        assert client.access_token == "new-access"
        assert client.refresh_token == "new-refresh"

    def test_refresh_raises_without_refresh_token(self, client):
        client.refresh_token = None
        with pytest.raises(WhoopAuthError, match="No refresh token"):
            asyncio.run(client._refresh_access_token())

    @patch("whoop_mcp.client.set_key")
    def test_refresh_raises_on_failure(self, mock_set_key, client):
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "invalid grant"

        mock_http_client = AsyncMock()
        mock_http_client.post.return_value = mock_response
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http_client):
            with pytest.raises(WhoopAuthError, match="Token refresh failed"):
                asyncio.run(client._refresh_access_token())

    @patch("whoop_mcp.client.set_key")
    def test_refresh_picks_up_token_rotated_by_sibling_process(self, mock_set_key, client, monkeypatch):
        """Regression: 2026-05-22 — webhook + 24h backend-restart raced.

        Old process refreshed and wrote RT2 to .env. New process spawning
        in the same window loaded .env BEFORE RT2 landed, so kept stale
        RT1 cached. WHOOP refresh tokens are single-use, so RT1 is invalid
        server-side. Next refresh from new process must read disk first
        and use whatever the sibling left there.
        """
        # Sibling already rotated to RT2 — disk has the new value
        monkeypatch.setattr(
            "whoop_mcp.client.dotenv_values",
            lambda _path: {
                "WHOOP_ACCESS_TOKEN": "sibling-rotated-access",
                "WHOOP_REFRESH_TOKEN": "RT2-sibling-rotated",
            },
        )
        # This process still has the stale RT1
        assert client.refresh_token == "test-refresh-token"

        captured_request_body = {}

        async def fake_post(_url, data=None, **_kwargs):
            captured_request_body.update(data or {})
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "access_token": "RT3-new-access",
                "refresh_token": "RT3-new-refresh",
            }
            return mock_response

        mock_http_client = AsyncMock()
        mock_http_client.post = fake_post
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http_client):
            asyncio.run(client._refresh_access_token())

        # The refresh request MUST have used RT2 (from disk), not RT1 (in-memory)
        assert captured_request_body["refresh_token"] == "RT2-sibling-rotated"
        # And of course the response then updates us further
        assert client.refresh_token == "RT3-new-refresh"


# --- API requests ---

class TestRequest:
    def _mock_http(self, status_code=200, json_data=None, text="error"):
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.json.return_value = json_data or {}
        mock_response.text = text

        mock_client = AsyncMock()
        mock_client.request.return_value = mock_response
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        return mock_client

    @patch("whoop_mcp.client.set_key")
    def test_successful_request(self, mock_set_key, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        mock_http = self._mock_http(json_data={"records": [{"id": 1}]})
        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            result = asyncio.run(client._request("GET", "/v2/recovery"))

        assert result == {"records": [{"id": 1}]}

    @patch("whoop_mcp.client.set_key")
    def test_rate_limit_raises(self, mock_set_key, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        mock_http = self._mock_http(status_code=429)
        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            with pytest.raises(WhoopAPIError, match="Rate limit"):
                asyncio.run(client._request("GET", "/v2/recovery"))

    @patch("whoop_mcp.client.set_key")
    def test_api_error_raises(self, mock_set_key, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        mock_http = self._mock_http(status_code=500, text="Internal Server Error")
        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            with pytest.raises(WhoopAPIError, match="API error 500"):
                asyncio.run(client._request("GET", "/v2/recovery"))


# --- Pagination ---

class TestPagination:
    @patch("whoop_mcp.client.set_key")
    def test_single_page(self, mock_set_key, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "records": [{"id": 1}, {"id": 2}],
            "next_token": None,
        }

        mock_http = AsyncMock()
        mock_http.request.return_value = mock_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            result = asyncio.run(client._paginated_request("/v2/recovery", limit=5))

        assert len(result) == 2

    @patch("whoop_mcp.client.set_key")
    def test_respects_limit(self, mock_set_key, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "records": [{"id": i} for i in range(10)],
            "next_token": None,
        }

        mock_http = AsyncMock()
        mock_http.request.return_value = mock_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            result = asyncio.run(client._paginated_request("/v2/recovery", limit=3))

        assert len(result) == 3


# --- High-level data methods ---

class TestDataMethods:
    @patch("whoop_mcp.client.set_key")
    def test_get_recovery(self, mock_set_key, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        record = {
            "cycle_id": 1,
            "user_id": 1,
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z",
            "score_state": "SCORED",
            "score": {
                "recovery_score": 80.0,
                "resting_heart_rate": 55.0,
                "hrv_rmssd_milli": 60.0,
            },
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"records": [record]}

        mock_http = AsyncMock()
        mock_http.request.return_value = mock_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            results = asyncio.run(client.get_recovery(limit=1))

        assert len(results) == 1
        assert results[0].score.recovery_score == 80.0

    @patch("whoop_mcp.client.set_key")
    def test_get_today_recovery_returns_none_when_empty(self, mock_set_key, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"records": []}

        mock_http = AsyncMock()
        mock_http.request.return_value = mock_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            result = asyncio.run(client.get_today_recovery())

        assert result is None

    @patch("whoop_mcp.client.set_key")
    def test_get_last_sleep_skips_naps(self, mock_set_key, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        records = [
            {
                "id": "nap-1",
                "user_id": 1,
                "created_at": "2025-01-01T14:00:00Z",
                "updated_at": "2025-01-01T14:30:00Z",
                "start": "2025-01-01T14:00:00Z",
                "end": "2025-01-01T14:30:00Z",
                "timezone_offset": "+00:00",
                "nap": True,
                "score_state": "SCORED",
            },
            {
                "id": "sleep-1",
                "user_id": 1,
                "created_at": "2025-01-01T22:00:00Z",
                "updated_at": "2025-01-02T06:00:00Z",
                "start": "2025-01-01T22:00:00Z",
                "end": "2025-01-02T06:00:00Z",
                "timezone_offset": "+00:00",
                "nap": False,
                "score_state": "SCORED",
            },
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"records": records}

        mock_http = AsyncMock()
        mock_http.request.return_value = mock_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            result = asyncio.run(client.get_last_sleep())

        assert result.id == "sleep-1"
        assert result.nap is False

    @patch("whoop_mcp.client.set_key")
    def test_get_last_sleep_returns_first_when_all_naps(self, mock_set_key, client):
        """When every sleep record is a nap, fall back to returning the first one."""
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        records = [
            {
                "id": "nap-1",
                "user_id": 1,
                "created_at": "2025-01-01T14:00:00Z",
                "updated_at": "2025-01-01T14:30:00Z",
                "start": "2025-01-01T14:00:00Z",
                "end": "2025-01-01T14:30:00Z",
                "timezone_offset": "+00:00",
                "nap": True,
                "score_state": "SCORED",
            },
            {
                "id": "nap-2",
                "user_id": 1,
                "created_at": "2025-01-01T15:00:00Z",
                "updated_at": "2025-01-01T15:30:00Z",
                "start": "2025-01-01T15:00:00Z",
                "end": "2025-01-01T15:30:00Z",
                "timezone_offset": "+00:00",
                "nap": True,
                "score_state": "SCORED",
            },
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"records": records}

        mock_http = AsyncMock()
        mock_http.request.return_value = mock_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            result = asyncio.run(client.get_last_sleep())

        assert result.id == "nap-1"
        assert result.nap is True

    @patch("whoop_mcp.client.set_key")
    def test_get_cycles(self, mock_set_key, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        record = {
            "id": 1,
            "user_id": 1,
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T12:00:00Z",
            "start": "2025-01-01T00:00:00Z",
            "timezone_offset": "+00:00",
            "score_state": "SCORED",
            "score": {
                "strain": 12.5,
                "kilojoule": 8000.0,
                "average_heart_rate": 70,
                "max_heart_rate": 155,
            },
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"records": [record]}

        mock_http = AsyncMock()
        mock_http.request.return_value = mock_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            results = asyncio.run(client.get_cycles(limit=7))

        assert len(results) == 1
        assert results[0].score.strain == 12.5

    @patch("whoop_mcp.client.set_key")
    def test_get_recovery_trend(self, mock_set_key, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        record = {
            "cycle_id": 1,
            "user_id": 1,
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z",
            "score_state": "SCORED",
            "score": {
                "recovery_score": 75.0,
                "resting_heart_rate": 52.0,
                "hrv_rmssd_milli": 70.0,
            },
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"records": [record]}

        mock_http = AsyncMock()
        mock_http.request.return_value = mock_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            results = asyncio.run(client.get_recovery_trend(days=7))

        assert len(results) == 1
        assert results[0].score.recovery_score == 75.0

    @patch("whoop_mcp.client.set_key")
    def test_get_workouts(self, mock_set_key, client):
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        record = {
            "id": "w-1",
            "user_id": 1,
            "created_at": "2025-01-01T10:00:00Z",
            "updated_at": "2025-01-01T11:00:00Z",
            "start": "2025-01-01T10:00:00Z",
            "end": "2025-01-01T11:00:00Z",
            "timezone_offset": "+00:00",
            "sport_name": "running",
            "score_state": "SCORED",
            "score": {
                "strain": 14.2,
                "average_heart_rate": 145,
                "max_heart_rate": 175,
                "kilojoule": 2500.0,
                "percent_recorded": 98.0,
                "distance_meter": 5000.0,
                "zone_durations": {
                    "zone_zero_milli": 0,
                    "zone_one_milli": 0,
                    "zone_two_milli": 0,
                    "zone_three_milli": 600000,
                    "zone_four_milli": 180000,
                    "zone_five_milli": 0,
                },
            },
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"records": [record]}

        mock_http = AsyncMock()
        mock_http.request.return_value = mock_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            results = asyncio.run(client.get_workouts(limit=5))

        assert len(results) == 1
        assert results[0].sport_name == "running"


# --- Ensure fresh token ---

class TestEnsureFreshToken:
    @patch("whoop_mcp.client.set_key")
    def test_refreshes_when_stale(self, mock_set_key, client):
        """ensure_fresh_token triggers refresh when token is stale."""
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = None

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "fresh-access",
            "refresh_token": "fresh-refresh",
        }

        mock_http = AsyncMock()
        mock_http.post.return_value = mock_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            asyncio.run(client.ensure_fresh_token())

        assert client.access_token == "fresh-access"

    def test_skips_when_fresh(self, client):
        """ensure_fresh_token does nothing when token was recently refreshed."""
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        # Should not raise or attempt refresh
        asyncio.run(client.ensure_fresh_token())
        assert client.access_token == "test-access-token"


# --- Request with token refresh ---

class TestRequestTokenRefresh:
    def _mock_http(self, status_code=200, json_data=None, text="error"):
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.json.return_value = json_data or {}
        mock_response.text = text

        mock_client = AsyncMock()
        mock_client.request.return_value = mock_response
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        return mock_client

    @patch("whoop_mcp.client.set_key")
    def test_proactive_refresh_on_stale_token(self, mock_set_key, client):
        """_request triggers proactive refresh when token is stale."""
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = None

        # First call: refresh token, second call: actual request
        mock_refresh_response = MagicMock()
        mock_refresh_response.status_code = 200
        mock_refresh_response.json.return_value = {
            "access_token": "refreshed-token",
            "refresh_token": "refreshed-refresh",
        }

        mock_request_response = MagicMock()
        mock_request_response.status_code = 200
        mock_request_response.json.return_value = {"data": "ok"}

        mock_http = AsyncMock()
        mock_http.post.return_value = mock_refresh_response
        mock_http.request.return_value = mock_request_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            result = asyncio.run(client._request("GET", "/v2/recovery"))

        assert result == {"data": "ok"}
        assert client.access_token == "refreshed-token"

    @patch("whoop_mcp.client.set_key")
    def test_401_triggers_retry_with_refresh(self, mock_set_key, client):
        """_request retries once after 401 by refreshing the token."""
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        # First request returns 401, then refresh succeeds, then retry succeeds
        mock_401_response = MagicMock()
        mock_401_response.status_code = 401

        mock_ok_response = MagicMock()
        mock_ok_response.status_code = 200
        mock_ok_response.json.return_value = {"data": "retried"}

        mock_refresh_response = MagicMock()
        mock_refresh_response.status_code = 200
        mock_refresh_response.json.return_value = {
            "access_token": "new-token",
            "refresh_token": "new-refresh",
        }

        mock_http = AsyncMock()
        mock_http.request.side_effect = [mock_401_response, mock_ok_response]
        mock_http.post.return_value = mock_refresh_response
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            result = asyncio.run(client._request("GET", "/v2/recovery"))

        assert result == {"data": "retried"}


# --- Multi-page pagination ---

class TestMultiPagePagination:
    @patch("whoop_mcp.client.set_key")
    def test_follows_next_token(self, mock_set_key, client):
        """Pagination follows next_token across multiple pages."""
        import whoop_mcp.client as client_mod
        client_mod._last_token_refresh = datetime.now()

        page1_response = MagicMock()
        page1_response.status_code = 200
        page1_response.json.return_value = {
            "records": [{"id": 1}, {"id": 2}],
            "next_token": "page2token",
        }

        page2_response = MagicMock()
        page2_response.status_code = 200
        page2_response.json.return_value = {
            "records": [{"id": 3}, {"id": 4}],
            "next_token": None,
        }

        mock_http = AsyncMock()
        mock_http.request.side_effect = [page1_response, page2_response]
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("whoop_mcp.client.httpx.AsyncClient", return_value=mock_http):
            result = asyncio.run(client._paginated_request("/v2/recovery", limit=10))

        assert len(result) == 4
        assert result[2]["id"] == 3
        # Verify nextToken was passed in second request
        second_call_params = mock_http.request.call_args_list[1]
        assert second_call_params.kwargs.get("params", {}).get("nextToken") == "page2token" or \
               (len(second_call_params.args) > 3 and "nextToken" in str(second_call_params))


# --- find_env_file ---

class TestFindEnvFile:
    def test_returns_default_when_no_env_exists(self):
        """Falls back to first location (project root) when no .env file is found."""
        with patch.object(Path, "exists", return_value=False):
            result = find_env_file()
        assert isinstance(result, Path)
        assert str(result).endswith(".env")
