#!/usr/bin/env python3
"""WHOOP refresh-token probe — an instrumented harness for the recurring
"lineage died overnight" failure.

WHY THIS EXISTS
---------------
Alix's WHOOP lineage has died repeatedly (2026-08-05, 08-07, 08-10). Every
post-mortem ends at the same wall: the logs record that a refresh FAILED with
``400 invalid_request``, but nothing records what the last SUCCESSFUL refresh
actually returned. So we have never been able to distinguish:

  (a) WHOOP returned a successor refresh token, we persisted it, and it was
      later invalidated by something else  -> a collision / orphan problem
  (b) WHOOP returned NO successor (the refresh grant omits ``scope=offline``),
      we kept the old token on disk, and the next refresh replayed a token
      the server had already consumed  -> a protocol bug, guaranteed death

This probe answers that by recording, for every single refresh:
the fingerprint of the token SENT, whether a successor came BACK, whether it
actually CHANGED, the granted scope, and the full error body on failure.

It is deliberately a standalone script, not a change to client.py: it must be
able to run against an isolated lineage without touching production behaviour.

SAFETY
------
A refresh is single-use and non-idempotent. This script therefore:

  * REFUSES to run against Alix's production token file (hard guard below).
  * Persists any successor to disk ATOMICALLY and IMMEDIATELY on receipt,
    before logging or anything else that could throw.
  * Uses a 60s timeout on the exchange and defers SIGINT/SIGTERM until the
    exchange completes -- an abort between the POST and the disk write is the
    exact mechanism that orphans a lineage.
  * Takes a lock file so two probes can never race each other.
  * Logs only SHA-256 fingerprints, never token values.

RATE LIMITS
-----------
WHOOP publishes 100 requests/minute and 10,000/day per client. The default
15-minute interval is ~96 refreshes/day (~192 requests/day with --api-check),
which is <2% of the daily allowance. Intervals below 60s are refused.

USAGE
-----
  # 1. Create an ISOLATED grant for the experiment (separate token file).
  #    get_tokens.py reads WHOOP_CLIENT_ID/SECRET *from* the token file, so
  #    seed those two lines first. Using the SAME client_id as production is
  #    deliberate -- a different OAuth app could mask an app-level scope
  #    misconfiguration, which is one of the things under test.
  cd ~/Code/repos/whoop-mcp
  grep -E '^WHOOP_CLIENT_(ID|SECRET)=' ~/.config/whoop/alix.env > ~/.config/whoop/probe.env
  chmod 600 ~/.config/whoop/probe.env
  WHOOP_TOKEN_FILE=~/.config/whoop/probe.env uv run python scripts/get_tokens.py

  # 2. One cycle, to check the harness works.
  uv run python scripts/refresh-probe.py --token-file ~/.config/whoop/probe.env \
      --once --api-check

  # 3. Run the experiment (arm A -- reproduces current production behaviour).
  uv run python scripts/refresh-probe.py --token-file ~/.config/whoop/probe.env \
      --scope-mode none --interval 900 --api-check

  # 4. When arm A dies: re-auth into the same file and run arm B.
  uv run python scripts/refresh-probe.py --token-file ~/.config/whoop/probe.env \
      --scope-mode offline --interval 900 --api-check

  # Read the results at any time (safe, no network).
  uv run python scripts/refresh-probe.py --analyze ~/.config/whoop/probe.env.probe.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
from dotenv import dotenv_values

TOKEN_URL = "https://api.prod.whoop.com/oauth/oauth2/token"
API_CHECK_URL = "https://api.prod.whoop.com/developer/v2/recovery"

# The refresh exchange must never be abandoned client-side. See client.py's
# TOKEN_REFRESH_TIMEOUT_SECONDS comment -- a short timeout here IS the orphan
# mechanism, not a protection against it.
REFRESH_TIMEOUT_SECONDS = 60.0
API_TIMEOUT_SECONDS = 15.0

MIN_INTERVAL_SECONDS = 60

# Token files this script must never touch. Alix's long-lived backend owns
# this lineage; a probe rotation would wedge production WHOOP data.
PRODUCTION_TOKEN_FILES = {
    Path("~/.config/whoop/alix.env").expanduser().resolve(strict=False),
}

# The scope WHOOP's authorization request uses. Ory Hydra (which WHOOP runs)
# returns a successor refresh token on the refresh grant only when the offline
# scope is carried through -- this is the hypothesis under test.
OFFLINE_SCOPE = "offline"


def fingerprint(value: Optional[str]) -> Optional[str]:
    """Stable short hash. Never log or persist raw token values."""
    if not value:
        return None
    return hashlib.sha256(value.encode()).hexdigest()[:12]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DeferredSignals:
    """Hold SIGINT/SIGTERM until the critical section completes.

    Ctrl-C landing between the WHOOP POST and the disk write would lose the
    successor and kill the lineage -- reproducing the bug we are measuring
    rather than measuring it.
    """

    def __init__(self) -> None:
        self.received: list[int] = []
        self._original: dict[int, Any] = {}

    def _handler(self, signum: int, _frame: Any) -> None:
        self.received.append(signum)
        print(f"\n[probe] signal {signum} received — finishing the token "
              f"exchange first, will exit after it completes.", flush=True)

    def __enter__(self) -> "DeferredSignals":
        for sig in (signal.SIGINT, signal.SIGTERM):
            self._original[sig] = signal.getsignal(sig)
            signal.signal(sig, self._handler)
        return self

    def __exit__(self, *_exc: Any) -> None:
        for sig, handler in self._original.items():
            signal.signal(sig, handler)


def atomic_write_env(path: Path, values: dict[str, str]) -> None:
    """Rewrite the token file atomically.

    python-dotenv's set_key does a read-modify-write per key, so a crash
    partway through can leave the access token updated and the refresh token
    not. One atomic replace keeps the file internally consistent.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    body = "".join(f"{k}={v}\n" for k, v in values.items())
    with open(tmp, "w") as fh:
        fh.write(body)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    os.chmod(path, 0o600)


def guard_token_file(path: Path) -> Path:
    resolved = path.expanduser().resolve(strict=False)
    if resolved in PRODUCTION_TOKEN_FILES:
        sys.exit(
            f"REFUSED: {resolved} is Alix's production token file.\n"
            "This probe rotates single-use tokens; running it here would wedge "
            "production WHOOP data.\nCreate an isolated grant instead:\n"
            f"  WHOOP_TOKEN_FILE=~/.config/whoop/probe.env uv run python scripts/get_tokens.py"
        )
    if not resolved.exists():
        sys.exit(
            f"Token file not found: {resolved}\n"
            "Create it first:\n"
            f"  WHOOP_TOKEN_FILE={resolved} uv run python scripts/get_tokens.py"
        )
    return resolved


def install_graceful_term() -> None:
    """Make SIGTERM exit the way Ctrl-C does, so the lock file is released.

    Two things conspire otherwise. Default SIGTERM handling kills the process
    outright, stranding the lock so the next probe refuses to start. And a
    process launched with `nohup ... &` from a non-interactive shell inherits
    SIG_IGN for SIGINT — so SIGTERM is often the only signal that will reach a
    backgrounded probe at all. Both observed 2026-08-11.

    Safe against the critical section: DeferredSignals replaces this handler
    for the duration of the token exchange and restores it afterwards.
    """
    def _raise(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _raise)


# Land this far AFTER the target second, never before it. Waking even a
# fraction early is not a rounding nicety here: a probe that fires at :59:59.8
# measures the second BEFORE the load arrives, so it understates the very spike
# it exists to measure, and its timestamp bins it into the previous minute.
# Observed 2026-08-12: `round()` on the remaining wait produced a repeating
# :45:00 / :45:59 / :47:00 / :47:59 pattern, leaving 9 minutes apparently
# unsampled and filing :30 and :15 readings under :29 and :14, which showed up
# as two spurious spikes.
OVERSHOOT_SECONDS = 0.10


def sleep_until(target_epoch: float) -> None:
    """Sleep to an absolute instant, accurately.

    time.sleep(n) only guarantees *at least* n, and a single long sleep drifts
    by however much the OS overshoots. Converging on the target — long sleep
    first, then progressively shorter ones — lands within a few milliseconds
    without spinning the CPU. Accuracy matters here because the effect being
    measured has a hard edge at the minute boundary: a sample fired a second
    early measures the quiet second before the load and understates the spike.
    """
    while True:
        remaining = target_epoch - time.time()
        if remaining <= 0:
            return
        time.sleep(remaining if remaining < 0.02 else remaining * 0.85)


def next_target_epoch(minutes: list[int], now: Optional[float] = None) -> float:
    """Absolute epoch of the next firing instant across a set of minutes."""
    now = time.time() if now is None else now
    return now + min(seconds_until_minute(m, now) for m in minutes)


def seconds_until_minute(target_minute: int, now: Optional[float] = None) -> float:
    """Seconds to wait until just after the next HH:MM:00 for a minute-of-hour.

    Deliberately overshoots by OVERSHOOT_SECONDS so the sample always lands
    inside the intended minute rather than at the tail of the previous one.
    """
    now = time.time() if now is None else now
    lt = time.localtime(now)
    # Seconds past the current hour, including the fractional part.
    past = lt.tm_min * 60 + lt.tm_sec + (now - int(now))
    target = target_minute * 60 + OVERSHOOT_SECONDS
    delta = target - past
    if delta <= 0:
        delta += 3600
    return delta


def sweep_minutes_for_hour(stride: int, hour: int) -> list[int]:
    """Which minutes this hour's slice of a rotating sweep covers.

    Hour H samples the minutes where ``m % stride == H % stride``, so each hour
    takes a different slice and every minute is covered once every ``stride``
    hours. With stride 5 that is 12 samples an hour instead of 60, and a full
    profile every 5 hours — a tenth of the request rate of sampling every minute,
    for the same eventual coverage.

    Rate matters here beyond politeness: these probes deliberately fail against
    the SAME client_id production uses, so anything that looked like abuse would
    take production down with it.
    """
    return [m for m in range(60) if m % stride == hour % stride]


def seconds_until_sweep(stride: int, now: Optional[float] = None) -> float:
    """Seconds until the next slot in a rotating sweep.

    Walks forward hour by hour rather than assuming the next slot is in the
    current hour: the last slot of one hour is often followed by a slot early in
    the next, and the slice changes as the hour rolls over.
    """
    now = time.time() if now is None else now
    lt = time.localtime(now)
    frac = now - int(now)
    for ahead in range(0, 25):
        hour = (lt.tm_hour + ahead) % 24
        for m in sweep_minutes_for_hour(stride, hour):
            # Seconds from `now` to that minute, `ahead` hours out.
            delta = (ahead * 3600) + ((m - lt.tm_min) * 60) - lt.tm_sec - frac + OVERSHOOT_SECONDS
            if delta > OVERSHOOT_SECONDS:
                return delta
    return 3600.0


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class ProbeLock:
    """Prevent two probes from racing the same lineage."""

    def __init__(self, token_file: Path) -> None:
        self.path = token_file.with_suffix(token_file.suffix + ".probe.lock")

    def _clear_if_stale(self) -> None:
        """Reclaim a lock whose owner is gone.

        A probe killed with SIGKILL (or SIGTERM before the handler above
        existed) leaves the file behind. Refusing to start forever because of a
        dead process's litter is worse than the race the lock guards against —
        especially for an experiment that must survive unattended for hours.
        """
        try:
            prior = self.path.read_text().strip()
        except OSError:
            return
        if prior.isdigit() and _pid_alive(int(prior)):
            return
        print(f"[probe] clearing stale lock from dead pid {prior or '?'}")
        self.path.unlink(missing_ok=True)

    def __enter__(self) -> "ProbeLock":
        self._clear_if_stale()
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            sys.exit(
                f"REFUSED: another probe holds {self.path} (pid in file).\n"
                "If that probe is dead, remove the lock file and retry."
            )
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.path.unlink(missing_ok=True)


def do_refresh(
    values: dict[str, str],
    scope_mode: str,
    token_file: Path,
    read_only: bool = False,
) -> dict[str, Any]:
    """One refresh exchange, fully instrumented.

    Returns the JSONL record. Persists a successor immediately on receipt.
    """
    sent_refresh = values.get("WHOOP_REFRESH_TOKEN", "")
    record: dict[str, Any] = {
        "ts": now_iso(),
        "scope_mode": scope_mode,
        "sent_refresh_fp": fingerprint(sent_refresh),
    }
    # Tag up front so latency scans are identifiable whatever the outcome. A
    # scan against a dead token 400s on every cycle by design, and those are
    # exactly the samples the by-minute profile is built from.
    if read_only:
        record["read_only"] = True

    data = {
        "grant_type": "refresh_token",
        "client_id": values.get("WHOOP_CLIENT_ID", ""),
        "client_secret": values.get("WHOOP_CLIENT_SECRET", ""),
        "refresh_token": sent_refresh,
    }
    # The lever under test. Arm "none" reproduces current production
    # behaviour (client.py sends no scope); arm "offline" adds it back.
    if scope_mode == "offline":
        data["scope"] = OFFLINE_SCOPE

    started = time.monotonic()
    try:
        response = httpx.post(TOKEN_URL, data=data, timeout=REFRESH_TIMEOUT_SECONDS)
    except Exception as err:  # noqa: BLE001 - we want every failure shape recorded
        record.update(
            ok=False,
            http_status=None,
            latency_ms=round((time.monotonic() - started) * 1000),
            transport_error=f"{type(err).__name__}: {err}",
        )
        return record

    record["latency_ms"] = round((time.monotonic() - started) * 1000)
    record["http_status"] = response.status_code

    if response.status_code != 200:
        record.update(ok=False, error_body=response.text[:1000])
        return record

    payload = response.json()
    new_access = payload.get("access_token")
    new_refresh = payload.get("refresh_token")

    # PERSIST FIRST. Everything after this point may throw; the successor must
    # already be durable. This is the whole lesson of the orphaned-token bugs.
    #
    # read_only exists for latency scanning against an ALREADY-DEAD token, where
    # every response is expected to be a 400 and there is nothing to save. But if
    # one unexpectedly succeeds we must still persist: refusing to write a
    # successor we have just been handed is precisely how a lineage gets
    # orphaned, and "I did not think this token was alive" is no defence.
    if read_only and not new_refresh:
        record.update(
            ok=True,
            returned_refresh_present=False, refresh_rotated=False,
            expires_in=payload.get("expires_in"), granted_scope=payload.get("scope"),
        )
        return record
    persisted = dict(values)
    if new_access:
        persisted["WHOOP_ACCESS_TOKEN"] = new_access
    if new_refresh:
        persisted["WHOOP_REFRESH_TOKEN"] = new_refresh
    expires_in = payload.get("expires_in")
    if expires_in:
        persisted["WHOOP_ACCESS_TOKEN_EXPIRES_AT"] = str(int(time.time()) + int(expires_in))
    atomic_write_env(token_file, persisted)

    record.update(
        ok=True,
        returned_refresh_present=bool(new_refresh),
        returned_refresh_fp=fingerprint(new_refresh),
        # THE headline measurement: did the lineage actually advance?
        refresh_rotated=bool(new_refresh) and new_refresh != sent_refresh,
        access_fp=fingerprint(new_access),
        expires_in=expires_in,
        granted_scope=payload.get("scope"),
        response_keys=sorted(payload.keys()),
    )
    return record


def do_control_probe(access_token: str) -> dict[str, Any]:
    """Time a plain API round-trip to the SAME host, immediately before the refresh.

    This is the control for the central confound. If a refresh after 90 minutes
    idle takes 14 seconds, that could be (a) WHOOP's token endpoint being slow
    on a cold grant, or (b) ordinary client-side cold start — DNS, TCP, TLS
    handshake — on a connection that has been idle just as long.

    Both requests go to api.prod.whoop.com over a fresh httpx client, so they
    pay identical DNS/TCP/TLS costs. Only the endpoint differs. A control that
    stays flat (~300ms) while the refresh climbs to seconds isolates the
    slowness to the token exchange itself.

    A 401 is a perfectly good measurement here — we want the round-trip time,
    not the payload, and after the access token expires 401 is expected.
    """
    started = time.monotonic()
    try:
        response = httpx.get(
            API_CHECK_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            params={"limit": 1},
            timeout=API_TIMEOUT_SECONDS,
        )
        return {"latency_ms": round((time.monotonic() - started) * 1000),
                "status": response.status_code}
    except Exception as err:  # noqa: BLE001
        return {"latency_ms": round((time.monotonic() - started) * 1000),
                "error": f"{type(err).__name__}: {err}"}


def access_token_expired(values: dict[str, str]) -> Optional[bool]:
    """Was the access token already past its expiry when we refreshed?

    The compact encoding of "cold": every death so far refreshed after expiry,
    every healthy probe cycle refreshed before it.
    """
    raw = values.get("WHOOP_ACCESS_TOKEN_EXPIRES_AT")
    if not raw:
        return None
    try:
        return time.time() >= int(raw)
    except ValueError:
        return None


def do_api_check(access_token: str) -> dict[str, Any]:
    """Confirm the freshly-minted access token actually works."""
    try:
        response = httpx.get(
            API_CHECK_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            params={"limit": 1},
            timeout=API_TIMEOUT_SECONDS,
        )
    except Exception as err:  # noqa: BLE001
        return {"ok": False, "transport_error": f"{type(err).__name__}: {err}"}
    out: dict[str, Any] = {"ok": response.status_code == 200, "status": response.status_code}
    if response.status_code != 200:
        out["body"] = response.text[:300]
    return out


def describe(record: dict[str, Any], cycle: int) -> str:
    if not record.get("ok"):
        detail = record.get("transport_error") or record.get("error_body", "")
        # Surface WHOOP's error code rather than the whole Hydra blurb.
        if isinstance(detail, str) and '"error"' in detail:
            try:
                detail = json.loads(detail).get("error", detail)
            except json.JSONDecodeError:
                pass
            detail = str(detail)[:80]
        return (f"#{cycle:<4} {record['ts']}  FAIL  "
                f"status={record.get('http_status')}  {detail}")

    rotated = record.get("refresh_rotated")
    flag = "ROTATED" if rotated else "*** NO SUCCESSOR ***"
    api = record.get("api_check", {})
    api_note = "" if not api else f"  api={api.get('status', api.get('transport_error'))}"
    return (f"#{cycle:<4} {record['ts']}  ok    "
            f"sent={record['sent_refresh_fp']} -> got={record.get('returned_refresh_fp')}  "
            f"{flag}  scope={record.get('granted_scope')!r}"
            f"  expires_in={record.get('expires_in')}{api_note}")


def analyze(path: Path) -> None:
    """Summarise a probe log. Pure local read -- no network, always safe."""
    if not path.exists():
        sys.exit(f"No probe log at {path}")
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not records:
        sys.exit(f"{path} is empty")

    ok = [r for r in records if r.get("ok")]
    failed = [r for r in records if not r.get("ok")]
    rotated = [r for r in ok if r.get("refresh_rotated")]
    no_successor = [r for r in ok if not r.get("refresh_rotated")]

    print(f"probe log: {path}")
    print(f"  window          {records[0]['ts']}  ->  {records[-1]['ts']}")
    print(f"  cycles          {len(records)}")
    print(f"  successful      {len(ok)}")
    print(f"    rotated       {len(rotated)}")
    print(f"    NO successor  {len(no_successor)}")
    print(f"  failed          {len(failed)}")

    by_arm: dict[str, dict[str, int]] = {}
    for r in records:
        arm = by_arm.setdefault(r.get("scope_mode", "?"),
                                {"ok": 0, "rotated": 0, "failed": 0})
        if r.get("ok"):
            arm["ok"] += 1
            if r.get("refresh_rotated"):
                arm["rotated"] += 1
        else:
            arm["failed"] += 1
    print("  by scope_mode:")
    for arm, counts in sorted(by_arm.items()):
        print(f"    {arm:<8} ok={counts['ok']:<4} rotated={counts['rotated']:<4} "
              f"failed={counts['failed']}")

    if failed:
        first = failed[0]
        print(f"\n  first failure   cycle {records.index(first) + 1} at {first['ts']}")
        print(f"    status        {first.get('http_status')}")
        body = first.get("error_body") or first.get("transport_error") or ""
        print(f"    body          {str(body)[:200]}")
        prior = [r for r in ok if r["ts"] < first["ts"]]
        if prior:
            last_ok = prior[-1]
            print(f"    last ok       {last_ok['ts']} "
                  f"(rotated={last_ok.get('refresh_rotated')})")

    # Ladder result: the whole point of escalating gaps is to bracket the idle
    # interval at which the lineage dies, so report it explicitly rather than
    # making the reader reconstruct it from timestamps.
    gapped = [r for r in records if r.get("idle_gap_minutes_before") is not None]
    if gapped:
        print("\n  IDLE-GAP LADDER")
        print(f"    {'idle':>6}  {'refresh':>9}  {'control':>9}  {'expired':>7}  result")
        for r in gapped:
            gap = r["idle_gap_minutes_before"]
            ctl = (r.get("control_probe") or {}).get("latency_ms")
            exp = r.get("access_token_expired_before")
            print(f"    {gap:>4}m   {str(r.get('latency_ms', '?'))+'ms':>9}  "
                  f"{(str(ctl)+'ms') if ctl is not None else '-':>9}  "
                  f"{'yes' if exp else ('no' if exp is False else '?'):>7}  "
                  f"{'ok' if r.get('ok') else 'FAILED'}")
        survived = [r["idle_gap_minutes_before"] for r in gapped if r.get("ok")]
        died = [r["idle_gap_minutes_before"] for r in gapped if not r.get("ok")]
        floor = max(survived) if survived else None
        ceiling = min(died) if died else None
        print()
        if floor is not None:
            print(f"    longest SURVIVED idle gap : {floor} min")
        if ceiling is not None:
            print(f"    shortest FATAL idle gap   : {ceiling} min")
            print(f"    -> boundary lies in ({floor if floor is not None else 0}, {ceiling}] minutes")
            safe = int((floor or 0) * 0.6) or 5
            print(f"    -> a keepalive refresh every ~{safe} min stays clear of it")
        else:
            print("    no fatal gap reached yet — the ladder has not found the ceiling")

    # Latency-by-minute profile. The whole point of the scan is a picture of
    # what happens around the top of the hour, so draw it rather than making
    # the reader reconstruct it from a column of numbers.
    scans = [r for r in records if r.get("read_only")]
    if scans:
        import statistics as _st
        by_min: dict[int, list[int]] = {}
        for r in scans:
            minute = int(r["ts"][14:16])
            if r.get("latency_ms") is not None:
                by_min.setdefault(minute, []).append(r["latency_ms"])
        allv = [v for vs in by_min.values() for v in vs]
        scale = max(allv) if allv else 1
        print("\n  TOKEN-ENDPOINT LATENCY BY MINUTE OF HOUR")
        print(f"    (n={len(scans)} samples, read-only, no grant consumed)")
        print()
        # Order so the hour boundary reads left-to-right: :58 :59 :00 :01 :02
        order = sorted(by_min, key=lambda m: (m - 30) % 60)
        for m in order:
            vs = by_min[m]
            med = int(_st.median(vs))
            bar = "#" * max(1, round(med / scale * 46))
            mark = "  <-- top of hour" if m == 0 else ""
            print(f"    :{m:02d}  {med:>6}ms  n={len(vs):<3} {bar}{mark}")
        if 0 in by_min:
            others = [v for m, vs in by_min.items() if m != 0 for v in vs]
            if others:
                ratio = _st.median(by_min[0]) / _st.median(others)
                print()
                print(f"    :00 is {ratio:.1f}x the median of every other minute sampled")

    print("\n  VERDICT")
    if no_successor and not rotated:
        print("    WHOOP never returned a successor refresh token. The token on")
        print("    disk is replayed every cycle -> lineage death is guaranteed.")
        print("    -> add scope=offline to the refresh grant in client.py")
    elif rotated and not no_successor:
        print("    Rotation works on every successful refresh. The refresh grant")
        print("    is NOT the bug -- look at what invalidates a good successor.")
    elif rotated and no_successor:
        print("    MIXED: rotation is intermittent. Correlate the no-successor")
        print("    cycles below against timing / concurrent activity.")
        for r in no_successor[:10]:
            print(f"      {r['ts']}  scope_mode={r.get('scope_mode')} "
                  f"granted_scope={r.get('granted_scope')!r}")
    else:
        print("    No successful refresh recorded yet.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Instrumented WHOOP refresh-token probe.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--token-file", type=Path,
                        help="Isolated token file. Never Alix's production file.")
    parser.add_argument("--interval", type=int, default=900,
                        help="Seconds between refreshes (default 900 = 15 min).")
    parser.add_argument("--burst", metavar="FROM:TO:EVERY",
                        help="One high-resolution pass across a boundary, in seconds relative "
                             "to the top of the hour: '-60:120:5' fires every 5s from :59:00 "
                             "to :01:00. Resolves the shape INSIDE the spike minute, which "
                             "per-minute sampling cannot — a queue clearing in 20s and one "
                             "clearing in 55s look identical at 1/min. Read-only only.")
    parser.add_argument("--sweep", type=int, metavar="STRIDE",
                        help="Rotating sweep: each hour samples the minutes where "
                             "m %% STRIDE == hour %% STRIDE, so every minute is covered "
                             "once every STRIDE hours at 60/STRIDE requests per hour. "
                             "STRIDE 5 gives a full-hour profile every 5 hours for a "
                             "fifth of the traffic of sampling every minute.")
    parser.add_argument("--at-minutes", metavar="MINS",
                        help="Fire at each listed minute past the hour, every hour, "
                             "e.g. '57,58,59,0,1,2,3'. Builds a latency profile across "
                             "the hour boundary. Combine with --read-only for a dead "
                             "token (free, unlimited) or leave it off for a live grant "
                             "(real refreshes, rotates each time).")
    parser.add_argument("--read-only", action="store_true",
                        help="Never persist a rotation; expect 400s and keep going. For "
                             "profiling the endpoint with an ALREADY-DEAD token, which "
                             "costs no grant. A successor is still persisted if one "
                             "unexpectedly arrives — dropping it would orphan the lineage.")
    parser.add_argument("--at-minute", type=int, metavar="M",
                        help="Refresh hourly at exactly HH:MM:00 (0-59). Tests whether "
                             "the minute-of-hour matters: every slow WHOOP call in 14 "
                             "days of production logs landed on :00 or :30, the cron "
                             "minutes, while identical calls elsewhere in the hour "
                             "averaged ~700ms. Pair --at-minute 0 against --at-minute 7 "
                             "on separate grants to isolate that from everything else.")
    parser.add_argument("--ladder", metavar="MINS",
                        help="Escalating gaps in minutes, e.g. '30,45,55,65,75'. Each "
                             "gap is used once, then the last value repeats. Finds the "
                             "idle interval at which the lineage dies. Overrides --interval.")
    parser.add_argument("--once", action="store_true", help="Single cycle, then exit.")
    parser.add_argument("--max-cycles", type=int, default=0,
                        help="Stop after N cycles (0 = run until failure or signal).")
    parser.add_argument("--scope-mode", choices=("none", "offline"), default="none",
                        help="'none' reproduces production; 'offline' tests the fix.")
    parser.add_argument("--control-probe", action="store_true",
                        help="Time a plain API round-trip to the same host immediately "
                             "BEFORE each refresh. Separates WHOOP-side token-endpoint "
                             "latency from ordinary client-side connection cold start — "
                             "the main confound in the cold-refresh hypothesis.")
    parser.add_argument("--api-check", action="store_true",
                        help="Also call the recovery endpoint to prove the access token works.")
    parser.add_argument("--stop-on-failure", action="store_true", default=True,
                        help="Halt on the first failed refresh (default: on).")
    parser.add_argument("--keep-going", dest="stop_on_failure", action="store_false",
                        help="Keep probing after a failure (records the dead-lineage shape).")
    parser.add_argument("--analyze", type=Path, metavar="LOG",
                        help="Summarise an existing probe log and exit. No network.")
    args = parser.parse_args()

    if args.analyze:
        analyze(args.analyze)
        return

    if not args.token_file:
        parser.error("--token-file is required (or use --analyze)")

    burst = None
    if args.burst:
        try:
            a, b, c = (int(x) for x in args.burst.split(":"))
        except ValueError:
            parser.error("--burst must be FROM:TO:EVERY in seconds, e.g. '-60:120:5'")
        if c < 2: parser.error("--burst interval must be at least 2 seconds")
        if b <= a: parser.error("--burst TO must be after FROM")
        if not args.read_only:
            parser.error("--burst requires --read-only: it fires far too often for a live grant")
        burst = (a, b, c)

    if args.sweep is not None:
        if not (1 <= args.sweep <= 60) or 60 % args.sweep != 0:
            parser.error("--sweep must be a divisor of 60 (e.g. 2, 3, 4, 5, 6, 10, 12)")
        if args.at_minutes or args.at_minute is not None or args.ladder:
            parser.error("--sweep cannot be combined with --at-minutes/--at-minute/--ladder")

    scan_minutes: list[int] = []
    if args.at_minutes:
        try:
            scan_minutes = sorted({int(x.strip()) for x in args.at_minutes.split(",") if x.strip()})
        except ValueError:
            parser.error("--at-minutes must be comma-separated minutes, e.g. '58,59,0,1,2'")
        if not scan_minutes or not all(0 <= m <= 59 for m in scan_minutes):
            parser.error("--at-minutes values must be between 0 and 59")
        if args.at_minute is not None or args.ladder:
            parser.error("--at-minutes cannot be combined with --at-minute or --ladder")

    if args.at_minute is not None and not (0 <= args.at_minute <= 59):
        parser.error("--at-minute must be between 0 and 59")
    if args.at_minute is not None and args.ladder:
        parser.error("--at-minute and --ladder are mutually exclusive")

    ladder: list[int] = []
    if args.ladder:
        try:
            ladder = [int(x.strip()) * 60 for x in args.ladder.split(",") if x.strip()]
        except ValueError:
            parser.error("--ladder must be comma-separated whole minutes, e.g. '30,45,65'")
        if not ladder:
            parser.error("--ladder was empty")
        if min(ladder) < MIN_INTERVAL_SECONDS:
            parser.error(f"--ladder entries must be >= {MIN_INTERVAL_SECONDS // 60} minute(s)")
    elif not args.once and args.interval < MIN_INTERVAL_SECONDS:
        parser.error(f"--interval must be >= {MIN_INTERVAL_SECONDS}s to stay well "
                     "inside WHOOP's published rate limits")

    install_graceful_term()

    token_file = guard_token_file(args.token_file)
    log_path = token_file.with_suffix(token_file.suffix + ".probe.jsonl")

    print(f"[probe] token file : {token_file}")
    print(f"[probe] log        : {log_path}")
    print(f"[probe] scope_mode : {args.scope_mode}"
          f"{'  (production behaviour)' if args.scope_mode == 'none' else '  (candidate fix)'}")
    if ladder:
        mins = ",".join(str(s // 60) for s in ladder)
        print(f"[probe] ladder     : {mins} min (then {ladder[-1] // 60} repeating)")
        print(f"[probe] purpose    : find the idle gap at which the lineage dies.")
        print(f"[probe]              every successful cycle raises the proven-safe floor;")
        print(f"[probe]              the first failure is the ceiling.")
    elif burst:
        a, b, c = burst
        n = (b - a) // c + 1
        print(f"[probe] burst      : every {c}s from {a:+d}s to {b:+d}s around the hour ({n} samples)")
        print("[probe] mode       : READ-ONLY — expects 400s, consumes no grant")
    elif args.sweep is not None:
        per_hour = 60 // args.sweep
        print(f"[probe] schedule   : rotating sweep, {per_hour} samples/hour, "
              f"every minute covered every {args.sweep}h ({per_hour * 24}/day)")
        print("[probe] mode       : " + ("READ-ONLY — expects 400s, consumes no grant"
                                         if args.read_only else "LIVE"))
    elif scan_minutes:
        print(f"[probe] schedule   : minutes {','.join(str(m) for m in scan_minutes)} of every hour")
        print("[probe] mode       : " + ("READ-ONLY — expects 400s, consumes no grant"
                                         if args.read_only else
                                         "LIVE — real refreshes, rotates the grant each cycle"))
    elif args.at_minute is not None:
        print(f"[probe] schedule   : hourly at :{args.at_minute:02d}:00 (24 refreshes/day)")
        print(f"[probe] purpose    : does the minute-of-hour drive the 502s?")
    else:
        print(f"[probe] interval   : {args.interval}s"
              f"  (~{round(86400 / args.interval)} refreshes/day)")
    print()

    # Read-only arms dispatch each cycle to a worker so a slow call cannot eat
    # the slots behind it. This is a correctness fix, not merely coverage: an
    # overrunning call swallows exactly the samples that follow it -- the ones
    # measuring how fast the endpoint recovers -- so the samples that survive
    # are biased towards looking healthy.
    #
    # A LIVE grant must never take this path. Refresh-token rotation makes
    # concurrent refreshes the precise race that orphans a lineage, so the gate
    # is --read-only and nothing else.
    async_cycles = bool(args.read_only)
    log_lock = threading.Lock()
    pool = ThreadPoolExecutor(max_workers=24, thread_name_prefix="probe") if async_cycles else None

    def run_cycle_async(cycle_no: int, values: dict, gap: Optional[int],
                        fired: Optional[float]) -> None:
        try:
            control = do_control_probe(values.get("WHOOP_ACCESS_TOKEN", "")) \
                if args.control_probe else None
            expired_before = access_token_expired(values)
            record = do_refresh(values, args.scope_mode, token_file, read_only=True)
            record["cycle"] = cycle_no
            record["access_token_expired_before"] = expired_before
            if control is not None:
                record["control_probe"] = control
            record["idle_gap_minutes_before"] = None if gap is None else round(gap / 60)
            record["fire_offset_ms"] = None if fired is None else round((fired % 60) * 1000)
            # The instant the cycle WOKE. `ts` is stamped after the control
            # probe, which is itself slow near the boundary, so `ts` is not a
            # safe basis for distance-from-the-hour analysis.
            record["fired_ts"] = (
                None if fired is None
                else datetime.fromtimestamp(fired, timezone.utc).isoformat()
            )
            line = json.dumps(record) + "\n"
            # Workers overlap by design, so the append must be serialised.
            with log_lock:
                with open(log_path, "a") as fh:
                    fh.write(line)
            print(describe(record, cycle_no), flush=True)
        except Exception as err:  # noqa: BLE001 - a dead worker must not be silent
            print(f"[probe] cycle {cycle_no} worker failed: "
                  f"{type(err).__name__}: {err}", flush=True)

    with ProbeLock(token_file):
        cycle = 0
        gap_before: Optional[int] = None
        fired_at: Optional[float] = None
        while True:
            cycle += 1
            values = dict(dotenv_values(token_file))
            if not values.get("WHOOP_REFRESH_TOKEN"):
                print("[probe] no WHOOP_REFRESH_TOKEN in token file — re-auth needed.")
                break

            if async_cycles:
                pool.submit(run_cycle_async, cycle, values, gap_before, fired_at)
            else:
                # Control FIRST, while the connection is as cold as the refresh
                # about to follow it. Ordering matters: run it after the refresh and
                # it would reuse a freshly-warmed path and prove nothing.
                control = do_control_probe(values.get("WHOOP_ACCESS_TOKEN", "")) \
                    if args.control_probe else None
                expired_before = access_token_expired(values)

                # The exchange and its persist are one indivisible unit.
                with DeferredSignals() as sigs:
                    record = do_refresh(values, args.scope_mode, token_file,
                                        read_only=args.read_only)
                    record["cycle"] = cycle
                    record["access_token_expired_before"] = expired_before
                    if control is not None:
                        record["control_probe"] = control
                    # The independent variable of the ladder experiment: how long
                    # the token sat idle before this refresh. Recorded on the cycle
                    # it *precedes* so a failure names the gap that killed it.
                    record["idle_gap_minutes_before"] = (
                        None if gap_before is None else round(gap_before / 60)
                    )
                    # Milliseconds past the minute boundary at which the cycle WOKE
                    # -- captured before the control probe and the refresh, or it
                    # would measure their duration instead of the timer's accuracy.
                    record["fire_offset_ms"] = (
                        None if fired_at is None else round((fired_at % 60) * 1000)
                    )
                    record["fired_ts"] = (
                        None if fired_at is None
                        else datetime.fromtimestamp(fired_at, timezone.utc).isoformat()
                    )
                    if record.get("ok") and args.api_check:
                        fresh = dict(dotenv_values(token_file))
                        record["api_check"] = do_api_check(fresh.get("WHOOP_ACCESS_TOKEN", ""))
                    with open(log_path, "a") as fh:
                        fh.write(json.dumps(record) + "\n")

                print(describe(record, cycle), flush=True)

                if sigs.received:
                    print("[probe] exiting on deferred signal.")
                    break
                if not record.get("ok") and args.stop_on_failure and not args.read_only:
                    print("\n[probe] refresh failed — stopping so the failure state is "
                          "preserved for inspection.")
                    print(f"[probe] summarise with:\n"
                          f"  uv run python scripts/refresh-probe.py --analyze {log_path}")
                    break
            if args.once:
                break
            if args.max_cycles and cycle >= args.max_cycles:
                print(f"[probe] reached --max-cycles {args.max_cycles}.")
                break

            # Ladder: use each gap once, then hold at the last (largest) value.
            # A successful cycle proves that gap is survivable; the first
            # failure brackets the boundary between it and the prior gap.
            target_epoch = None
            if burst:
                a, b, c = burst
                now = time.time()
                lt = time.localtime(now)
                # Seconds to the next top-of-hour, then walk the offset ladder.
                secs_into = (lt.tm_min * 60 + lt.tm_sec) + (now - int(now))
                to_hour = 3600 - secs_into
                offsets = list(range(a, b + 1, c))
                # Anchor on whichever boundary the window still reaches. Once the
                # clock passes :00, the boundary we care about is behind us, so
                # its positive offsets are the ones still to fire -- measuring
                # only to_hour restarts the ladder at FROM and loses every
                # post-boundary sample, which is the whole drain curve.
                cands = [base + o
                         for base in (-secs_into, to_hour)
                         for o in offsets
                         if base + o > 0.4]
                nxt = min(cands) if cands else to_hour + 3600 + offsets[0]
                target_epoch = now + nxt
                gap_before = math.ceil(nxt)
                if gap_before > 90:
                    print(f"[probe] waiting {gap_before}s for the boundary window", flush=True)
            elif args.sweep is not None:
                gap_before = math.ceil(seconds_until_sweep(args.sweep))
                target_epoch = time.time() + seconds_until_sweep(args.sweep)
                print(f"[probe] next sweep sample in {gap_before}s", flush=True)
            elif scan_minutes:
                waits = [seconds_until_minute(m) for m in scan_minutes]
                gap_before = math.ceil(min(waits))
                target_epoch = time.time() + min(waits)
                nxt = scan_minutes[waits.index(min(waits))]
                print(f"[probe] next scan at :{nxt:02d} ({gap_before}s)", flush=True)
            elif args.at_minute is not None:
                gap_before = math.ceil(seconds_until_minute(args.at_minute))
                target_epoch = time.time() + seconds_until_minute(args.at_minute)
                print(f"[probe] sleeping {gap_before}s until :{args.at_minute:02d}:00", flush=True)
            elif ladder:
                gap_before = ladder[cycle - 1] if cycle - 1 < len(ladder) else ladder[-1]
                print(f"[probe] next gap: {gap_before // 60} min "
                      f"(proven safe so far: {(ladder[cycle - 2] // 60) if cycle >= 2 else 0} min)",
                      flush=True)
            else:
                gap_before = args.interval

            try:
                if target_epoch is not None:
                    sleep_until(target_epoch)
                else:
                    time.sleep(gap_before)
                fired_at = time.time()
            except KeyboardInterrupt:
                print("\n[probe] interrupted while idle — safe to stop.")
                break

        # Let dispatched cycles finish before the summary reads the log, or the
        # last few samples are counted as missing rather than merely in flight.
        if pool is not None:
            print("[probe] draining in-flight cycles...", flush=True)
            pool.shutdown(wait=True)

    print()
    analyze(log_path)


if __name__ == "__main__":
    main()
