"""WHOOP API client with automatic token refresh."""

import asyncio
import hashlib
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import httpx
from dotenv import dotenv_values, load_dotenv, set_key

from whoop_mcp.models import Recovery, Sleep, Cycle, Workout

# Find .env file (check multiple locations)
def find_env_file() -> Path:
    """Resolve which dotenv file holds WHOOP credentials + tokens.

    Priority:
      1. $WHOOP_TOKEN_FILE (if set and non-empty) — lets independent consumers
         keep separate, non-colliding refresh-token lineages. WHOOP rotates the
         refresh token on every use and invalidates the prior one, so two
         long-lived processes sharing a single token file (e.g. the Alix harness
         and a conversational Claude Code agent, both spawning this server from
         the same checkout) rotate each other's tokens out from under them and
         eventually wedge until manual re-auth. A dedicated token file per
         OAuth authorization avoids the collision entirely.
      2. <project root>/.env
      3. <cwd>/.env
    """
    override = os.getenv("WHOOP_TOKEN_FILE")
    if override and override.strip():
        # Return unconditionally (may not exist yet — get_tokens.py creates it).
        return Path(override).expanduser()
    locations = [
        Path(__file__).parent.parent.parent / ".env",  # Project root
        Path.cwd() / ".env",  # Current directory
    ]
    for path in locations:
        if path.exists():
            return path
    return locations[0]  # Default to project root


ENV_PATH = find_env_file()
load_dotenv(ENV_PATH)

# Track when we last refreshed the token (persists across tool calls within MCP session)
_last_token_refresh: Optional[datetime] = None
_refresh_lock = asyncio.Lock()
TOKEN_LIFETIME_MINUTES = 55  # Refresh proactively before the 60-min expiry
# Refresh this far ahead of the persisted access-token expiry (clock skew / in-flight margin).
EXPIRY_SAFETY_MARGIN = timedelta(minutes=5)
# Timeout for ordinary data requests to the WHOOP API. Bounded so a hung
# endpoint fails the tool call promptly rather than stalling a context source.
HTTP_TIMEOUT_SECONDS = 12.0

# Timeout for the OAuth refresh POST specifically — deliberately far longer.
#
# A refresh is a SINGLE-USE, NON-IDEMPOTENT exchange: the instant WHOOP receives
# the POST it invalidates the old refresh token and mints a successor. Any
# client-side abort after that point loses the successor forever and wedges the
# lineage with "invalid_request" (note: NOT "invalid_grant") until manual
# re-auth. So there is no such thing as a safe client-side timeout here — the
# only correct move is to wait for the answer.
#
# The earlier 12.0 value was chosen to stay under Alix's 15s per-context-source
# timeout, on the theory that the tool call and the refresh had to succeed or
# fail together. That was the bug: it made our own timeout the orphan mechanism.
# Observed live 2026-08-05T12:00Z (UTC) on the 1pm halftime run — the POST hit
# 12.0s and raised (an empty-message httpx timeout in the logs), the retry 12s
# later got "invalid_request", and the next morning's morning-checkin ran with
# no WHOOP data. Third token death by the same family of cause.
#
# Decoupling the two is safe because _refresh_access_token runs the exchange
# under asyncio.shield: the caller's tool call can time out and fail fast while
# the shielded exchange runs to completion and persists the successor to disk,
# so the NEXT call picks up a valid token. A slow refresh success always beats
# an orphaned token — one failed context source costs a single agent run, an
# orphan costs every WHOOP-dependent run until a human re-auths.
TOKEN_REFRESH_TIMEOUT_SECONDS = 60.0


def _token_fp(value: Optional[str]) -> str:
    """Short, stable fingerprint of a token — safe to log.

    Never log token values. A fingerprint is all that is needed to answer the
    only question that matters after the fact: did this value change?
    """
    if not value:
        return "none"
    return hashlib.sha256(value.encode()).hexdigest()[:12]


def _log(message: str) -> None:
    """Log to stderr — stdout is the MCP protocol channel and must stay clean.

    Alix captures this as ``[whoop:stderr]`` in its process log, so these lines
    land in the same place as the refresh failures they explain.

    The pid is in every line on purpose. The failure mode these logs exist to
    catch is two processes sharing one single-use token lineage, and that is
    unreadable unless each line says who emitted it.
    """
    print(f"[whoop pid={os.getpid()}] {message}", file=sys.stderr, flush=True)


# Announce, at import, which process is bound to which lineage.
#
# "Who else is holding this token file?" is the first question in every WHOOP
# token post-mortem, and until now it could only be inferred after the fact.
# One line per spawn makes it greppable: the resolved token file, whether
# WHOOP_TOKEN_FILE steered us there, and the fingerprint this process starts
# from. Two processes announcing the SAME file is a collision — visible
# immediately instead of days later via a dead lineage.
_log(
    f"start token_file={ENV_PATH} "
    f"override={'WHOOP_TOKEN_FILE' if os.getenv('WHOOP_TOKEN_FILE') else 'none'} "
    f"refresh_fp={_token_fp(os.getenv('WHOOP_REFRESH_TOKEN'))}"
)


def _read_persisted_access_token_expiry() -> Optional[datetime]:
    """Read WHOOP_ACCESS_TOKEN_EXPIRES_AT (epoch seconds) from the token file.

    Lets a freshly-spawned process decide whether the on-disk access token is
    still usable WITHOUT proactively rotating the single-use refresh token.
    Returns None when the field is absent or unparseable (older token files that
    predate this field), in which case callers fall back to the historical
    proactive-refresh behavior.
    """
    try:
        raw = dotenv_values(ENV_PATH).get("WHOOP_ACCESS_TOKEN_EXPIRES_AT")
    except OSError:
        return None
    if not raw:
        return None
    try:
        return datetime.fromtimestamp(int(raw))
    except (ValueError, OverflowError, OSError):
        return None


class WhoopAuthError(Exception):
    """Authentication error with WHOOP API."""
    pass


class WhoopAPIError(Exception):
    """General WHOOP API error."""
    pass


class WhoopClient:
    """Async client for WHOOP API with automatic token refresh."""

    BASE_URL = "https://api.prod.whoop.com/developer"
    TOKEN_URL = "https://api.prod.whoop.com/oauth/oauth2/token"

    def __init__(self):
        """Initialize the client with tokens from environment."""
        self.client_id = os.getenv("WHOOP_CLIENT_ID")
        self.client_secret = os.getenv("WHOOP_CLIENT_SECRET")
        self.access_token = os.getenv("WHOOP_ACCESS_TOKEN")
        self.refresh_token = os.getenv("WHOOP_REFRESH_TOKEN")

        if not self.access_token:
            raise WhoopAuthError(
                "No access token found. Run 'uv run python scripts/get_tokens.py' first."
            )

    def _token_needs_refresh(self) -> bool:
        """Check if the token should be proactively refreshed.

        A fresh process resets the in-memory ``_last_token_refresh`` to None.
        Historically that forced a proactive refresh on the FIRST call of every
        spawn. Because WHOOP rotates — and immediately invalidates — the refresh
        token on every use, frequent backend respawns (idle blue-green restarts,
        per-webhook context fetches) rotated the single-use lineage far more often
        than the ~hourly access-token lifetime actually required. Each extra
        rotation is another chance for the successor to be lost to a process
        teardown between the WHOOP POST and the disk persist, wedging the whole
        lineage until manual re-auth.

        To avoid needless rotation, a fresh process now consults the persisted
        access-token expiry: if the on-disk token is still valid we skip the
        refresh entirely and let the request use it (the 401 fallback in
        ``_request`` still covers a genuinely-expired token). Only when the expiry
        is unknown do we fall back to the old proactive-refresh behavior.
        """
        # Each branch logs only when it decides to ROTATE. A rotation is the
        # risky operation (single-use token), so the reason for every one of
        # them needs to be on the record — particularly the third branch, which
        # rotates without any evidence that the current token has expired.
        global _last_token_refresh
        if _last_token_refresh is not None:
            elapsed = datetime.now() - _last_token_refresh
            due = elapsed > timedelta(minutes=TOKEN_LIFETIME_MINUTES)
            if due:
                _log(
                    f"refresh due: token refreshed in-process "
                    f"{elapsed.total_seconds() / 60:.0f}m ago (limit {TOKEN_LIFETIME_MINUTES}m)"
                )
            return due

        expires_at = _read_persisted_access_token_expiry()
        if expires_at is not None:
            due = datetime.now() >= (expires_at - EXPIRY_SAFETY_MARGIN)
            if due:
                _log(
                    f"refresh due: fresh process, on-disk access token expired/expiring at "
                    f"{expires_at.isoformat(timespec='seconds')} "
                    f"(safety margin {EXPIRY_SAFETY_MARGIN})"
                )
            return due
        # Unknown expiry (token file predates this field) — preserve the historical
        # proactive-refresh-on-first-call behavior.
        _log(
            "refresh due: fresh process and NO persisted expiry on disk — falling back "
            "to proactive refresh. This rotates the single-use token without knowing "
            "the current one had expired; frequent respawns here burn the lineage."
        )
        return True

    async def ensure_fresh_token(self) -> None:
        """Ensure token is fresh before making concurrent requests.

        Call this once before asyncio.gather() to avoid race conditions
        where multiple concurrent requests all try to refresh simultaneously.
        """
        if self._token_needs_refresh():
            await self._refresh_access_token()

    async def _refresh_access_token(self) -> None:
        """Refresh the access token using the refresh token.

        The token exchange + disk persist runs under asyncio.shield so a
        cancellation of the *caller* cannot interrupt it mid-flight. WHOOP
        rotates (and immediately invalidates) the refresh token the instant
        the POST is received; if this coroutine were cancelled between that
        POST and set_key() persisting the successor, the rotated token would
        be lost and the lineage wedged with "invalid_request" until manual
        re-auth.

        That exact abort happened live 2026-07-28T12:00Z (UTC): a cold-started
        whoop backend's lazy refresh exceeded alix's 5s per-context-source
        timeout, the context assembler aborted the tool call mid-exchange, and
        the rotated token was never written to disk — silently killing the
        next morning's morning-checkin. The process survives the abort (only
        the request coroutine is cancelled), so shielding lets the exchange
        run to completion regardless, keeping disk and WHOOP in lock-step.
        """
        await asyncio.shield(self._refresh_access_token_locked())

    async def _refresh_access_token_locked(self) -> None:
        """Serialized token exchange. Never call directly — go through
        _refresh_access_token so the critical section is cancellation-shielded.
        """
        global _last_token_refresh

        async with _refresh_lock:
            # Double-check: another coroutine may have refreshed while we waited
            if not self._token_needs_refresh():
                self.access_token = os.environ.get("WHOOP_ACCESS_TOKEN", self.access_token)
                self.refresh_token = os.environ.get("WHOOP_REFRESH_TOKEN", self.refresh_token)
                return

            # Cross-process safety: a sibling whoop-mcp process (e.g. spawned
            # by alix's 24h backend-restart timer while a webhook was in
            # flight) may have rotated the refresh token after this process
            # cached it at __init__. WHOOP refresh tokens are single-use —
            # the server invalidates the old one on rotation — so issuing
            # our refresh with a stale token leaves the agent wedged until
            # manual re-auth.
            #
            # Observed live 2026-05-22T09:05Z (UTC): workout webhook +
            # stale-backend restart raced. The new process kept stale RT1
            # in memory for 10h until its next refresh, then failed with
            # "Token refresh failed: invalid_request" and blocked the next
            # morning's morning-checkin.
            #
            # Use dotenv_values (returns a dict, doesn't mutate os.environ)
            # so tests can patch this hook without polluting global env state.
            disk_vars = dotenv_values(ENV_PATH)
            disk_refresh = disk_vars.get("WHOOP_REFRESH_TOKEN")
            if disk_refresh and disk_refresh != self.refresh_token:
                # THE cross-process collision signal. This branch firing means
                # some other process rotated the lineage while we held a stale
                # copy — i.e. two consumers are sharing one single-use token.
                # It was previously silent, so every past investigation could
                # only infer sharing after the lineage was already dead.
                _log(
                    f"sibling rotation detected: on-disk refresh token "
                    f"{_token_fp(disk_refresh)} != in-memory {_token_fp(self.refresh_token)} "
                    f"— adopting the disk copy. Another process is writing {ENV_PATH}."
                )
                self.refresh_token = disk_refresh
                self.access_token = disk_vars.get("WHOOP_ACCESS_TOKEN") or self.access_token

            if not self.refresh_token:
                raise WhoopAuthError("No refresh token available. Re-run get_tokens.py")

            # Captured before the exchange so we can tell afterwards whether the
            # lineage actually ADVANCED. Five separate token deaths (2026-05-22,
            # 07-28, 08-05, 08-07, 08-10) were investigated without this fact,
            # because the logs only ever recorded refresh FAILURES — never what
            # the last SUCCESSFUL refresh returned. That made two very different
            # causes indistinguishable: a good successor invalidated by something
            # else, versus no successor ever being issued.
            sent_refresh = self.refresh_token

            async with httpx.AsyncClient(timeout=TOKEN_REFRESH_TIMEOUT_SECONDS) as client:
                response = await client.post(
                    self.TOKEN_URL,
                    data={
                        "grant_type": "refresh_token",
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                        "refresh_token": self.refresh_token,
                    },
                )

                if response.status_code != 200:
                    # Fingerprint the REJECTED token so a later post-mortem can
                    # tell whether WHOOP refused the token we last persisted (a
                    # server-side invalidation) or a stale one we were still
                    # holding (a local caching / collision bug).
                    _log(
                        f"token refresh FAILED status={response.status_code} "
                        f"sent_fp={_token_fp(sent_refresh)} — {response.text[:200]}"
                    )
                    raise WhoopAuthError(f"Token refresh failed: {response.text}")

                tokens = response.json()
                returned_refresh = tokens.get("refresh_token")
                self.access_token = tokens["access_token"]
                self.refresh_token = tokens.get("refresh_token", self.refresh_token)
                _last_token_refresh = datetime.now()  # Record refresh time

                # Persist the access-token expiry so a future fresh process can tell
                # whether the on-disk token is still usable WITHOUT rotating the
                # single-use refresh token. WHOOP returns expires_in (seconds).
                expires_at_epoch: Optional[int] = None
                expires_in = tokens.get("expires_in")
                if expires_in:
                    try:
                        expires_at_epoch = int(
                            (_last_token_refresh + timedelta(seconds=int(expires_in))).timestamp()
                        )
                    except (ValueError, TypeError):
                        expires_at_epoch = None

                # Save new tokens to .env (quote_mode="never" prevents quote issues)
                set_key(str(ENV_PATH), "WHOOP_ACCESS_TOKEN", self.access_token, quote_mode="never")
                if tokens.get("refresh_token"):
                    set_key(str(ENV_PATH), "WHOOP_REFRESH_TOKEN", self.refresh_token, quote_mode="never")
                if expires_at_epoch is not None:
                    set_key(str(ENV_PATH), "WHOOP_ACCESS_TOKEN_EXPIRES_AT", str(expires_at_epoch), quote_mode="never")

                # THE diagnostic line. A refresh that succeeds but returns no
                # successor leaves the just-consumed token on disk, so the NEXT
                # refresh replays it and WHOOP answers 400 invalid_request —
                # the recurring "lineage died overnight" failure. Until now that
                # state was indistinguishable from a healthy refresh: the file
                # mtime still moves (the access token and expiry are written
                # either way), so even the staleness tripwire is fooled. The
                # damage only surfaces hours later as an empty daily note.
                if returned_refresh and returned_refresh != sent_refresh:
                    _log(
                        f"token refresh ok: lineage advanced "
                        f"{_token_fp(sent_refresh)} -> {_token_fp(returned_refresh)} "
                        f"scope={tokens.get('scope')!r} expires_in={expires_in}"
                    )
                else:
                    reason = "same value returned" if returned_refresh else "absent from response"
                    _log(
                        f"token refresh ok BUT NO SUCCESSOR ({reason}) — refresh token "
                        f"unchanged, fp={_token_fp(sent_refresh)}, scope={tokens.get('scope')!r}. "
                        "The next refresh will replay a consumed token and the lineage "
                        "will die with invalid_request."
                    )

                # Also update in-memory env vars so new WhoopClient instances get fresh tokens
                os.environ["WHOOP_ACCESS_TOKEN"] = self.access_token
                os.environ["WHOOP_REFRESH_TOKEN"] = self.refresh_token
                if expires_at_epoch is not None:
                    os.environ["WHOOP_ACCESS_TOKEN_EXPIRES_AT"] = str(expires_at_epoch)

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        retry_on_401: bool = True,
    ) -> dict:
        """Make an authenticated request to the WHOOP API."""
        # Proactively refresh if token is stale (avoids wasted 401 round-trip)
        if self._token_needs_refresh():
            await self._refresh_access_token()

        url = f"{self.BASE_URL}{endpoint}"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
            response = await client.request(method, url, headers=headers, params=params)

            # Fallback: handle unexpected token expiration
            if response.status_code == 401 and retry_on_401:
                await self._refresh_access_token()
                return await self._request(method, endpoint, params, retry_on_401=False)

            if response.status_code == 429:
                raise WhoopAPIError("Rate limit exceeded. Try again later.")

            if response.status_code != 200:
                raise WhoopAPIError(f"API error {response.status_code}: {response.text}")

            return response.json()

    async def _paginated_request(
        self,
        endpoint: str,
        limit: int,
        max_per_page: int = 25,
    ) -> list[dict]:
        """Fetch paginated results up to the requested limit."""
        all_records = []
        next_token = None

        while len(all_records) < limit:
            page_limit = min(max_per_page, limit - len(all_records))
            params = {"limit": page_limit}
            if next_token:
                params["nextToken"] = next_token

            data = await self._request("GET", endpoint, params=params)
            records = data.get("records", [])
            all_records.extend(records)

            next_token = data.get("next_token")
            if not next_token or not records:
                break

        return all_records[:limit]

    async def get_recovery(self, limit: int = 1) -> list[Recovery]:
        """Get recent recovery records (supports up to 30 days via pagination)."""
        records = await self._paginated_request("/v2/recovery", limit)
        return [Recovery.model_validate(record) for record in records]

    async def get_today_recovery(self) -> Optional[Recovery]:
        """Get today's recovery data."""
        records = await self.get_recovery(limit=1)
        return records[0] if records else None

    async def get_sleep(self, limit: int = 1) -> list[Sleep]:
        """Get recent sleep records (supports up to 30 days via pagination)."""
        records = await self._paginated_request("/v2/activity/sleep", limit)
        return [Sleep.model_validate(record) for record in records]

    async def get_last_sleep(self) -> Optional[Sleep]:
        """Get the most recent sleep record (main sleep, not nap)."""
        # Get more records to find main sleep
        records = await self.get_sleep(limit=5)
        for record in records:
            if not record.nap:
                return record
        return records[0] if records else None

    async def get_cycles(self, limit: int = 7) -> list[Cycle]:
        """Get recent physiological cycles (for strain data)."""
        data = await self._request("GET", "/v2/cycle", params={"limit": limit})
        return [Cycle.model_validate(record) for record in data.get("records", [])]

    async def get_recovery_trend(self, days: int = 7) -> list[Recovery]:
        """Get recovery trend for the last N days."""
        return await self.get_recovery(limit=days)

    async def get_workouts(self, limit: int = 10) -> list[Workout]:
        """Get recent workout records (supports pagination for full history)."""
        records = await self._paginated_request("/v2/activity/workout", limit)
        return [Workout.model_validate(record) for record in records]
