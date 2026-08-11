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
import os
import signal
import sys
import time
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
    else:
        print(f"[probe] interval   : {args.interval}s"
              f"  (~{round(86400 / args.interval)} refreshes/day)")
    print()

    with ProbeLock(token_file):
        cycle = 0
        gap_before: Optional[int] = None
        while True:
            cycle += 1
            values = dict(dotenv_values(token_file))
            if not values.get("WHOOP_REFRESH_TOKEN"):
                print("[probe] no WHOOP_REFRESH_TOKEN in token file — re-auth needed.")
                break

            # Control FIRST, while the connection is as cold as the refresh
            # about to follow it. Ordering matters: run it after the refresh and
            # it would reuse a freshly-warmed path and prove nothing.
            control = do_control_probe(values.get("WHOOP_ACCESS_TOKEN", "")) \
                if args.control_probe else None
            expired_before = access_token_expired(values)

            # The exchange and its persist are one indivisible unit.
            with DeferredSignals() as sigs:
                record = do_refresh(values, args.scope_mode, token_file)
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
                if record.get("ok") and args.api_check:
                    fresh = dict(dotenv_values(token_file))
                    record["api_check"] = do_api_check(fresh.get("WHOOP_ACCESS_TOKEN", ""))
                with open(log_path, "a") as fh:
                    fh.write(json.dumps(record) + "\n")

            print(describe(record, cycle), flush=True)

            if sigs.received:
                print("[probe] exiting on deferred signal.")
                break
            if not record.get("ok") and args.stop_on_failure:
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
            if ladder:
                gap_before = ladder[cycle - 1] if cycle - 1 < len(ladder) else ladder[-1]
                print(f"[probe] next gap: {gap_before // 60} min "
                      f"(proven safe so far: {(ladder[cycle - 2] // 60) if cycle >= 2 else 0} min)",
                      flush=True)
            else:
                gap_before = args.interval

            try:
                time.sleep(gap_before)
            except KeyboardInterrupt:
                print("\n[probe] interrupted while idle — safe to stop.")
                break

    print()
    analyze(log_path)


if __name__ == "__main__":
    main()
