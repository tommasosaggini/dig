"""
DIG — Global Spotify call gate (the single "drip-lane").

Spotify Extended Quota is NOT available to us, so Development Mode is permanent
and its quota is tiny. **The ban is PER ENDPOINT, not per app** — measured
2026-08-18, `artists/{id}/albums` 22 hours into a 429 while `artists/{id}`,
`albums/{id}/tracks` and `search` all answered 200 on the same token, the same
second. So the strategy is: never burst, trickle, and quarantine the endpoint
that got banned instead of the whole app.

Every Spotify API call across every script flows through one `GatedSpotify`
client. Its `_internal_call` (spotipy's single HTTP chokepoint) does, in order:

  1. COOLDOWN — if THIS CALL'S endpoint family is in a recorded 429 cooldown,
     raise a synthetic 429 immediately (no network call). Existing per-script
     429 handlers then abort cleanly, exactly as they do for a real 429. A ban
     on one family leaves the others working: while the album walk is locked
     out, the likes sync still reaches `me/library`, which used to sit out the
     full 22 hours for nothing.
  2. PACE — block until MIN_INTERVAL has elapsed since the last global call,
     enforced CROSS-PROCESS via an fcntl lock on a shared timestamp file. All
     cron jobs `docker exec` into the same container, so they share /tmp and
     pace against each other.
  3. CALL — make the request; on a real 429, persist the cooldown via
     `spotify_health.record_429` so every other job short-circuits.

Combined with a shared cron flock (only one Spotify job runs at a time), this
turns discovery into a steady trickle that never trips the lockout.

Drop-in usage — replace `spotipy.Spotify(...)` with `make_client(...)`:
    from lib.spotify_gate import make_client
    sp = make_client()                       # client-credentials (default)
    sp = make_client(auth=user_token)        # user-token flow

Tune the pace without redeploying via env var DIG_SPOTIFY_MIN_INTERVAL (seconds).
"""

import fcntl
import json
import logging
import os
import re
import time

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from lib import spotify_health

# Global minimum seconds between ANY two Spotify calls (cross-process).
# Conservative default; tune empirically once the lockout clears.
MIN_INTERVAL = float(os.environ.get("DIG_SPOTIFY_MIN_INTERVAL", "1.5"))
# Never block a single call longer than this just for pacing (a safety cap so a
# corrupt timestamp can't wedge a job forever).
MAX_PACE_SLEEP = 30.0

PACE_PATH = "/tmp/dig_spotify_pace.json"
PACE_LOCK = "/tmp/dig_spotify_pace.lock"


def cooldown_remaining(family=spotify_health.DEFAULT_FAMILY):
    """Seconds left on THIS FAMILY's recorded 429 cooldown (0 if clear).
    Cheap file read — never probes Spotify."""
    try:
        return spotify_health.cooldown_for(family)
    except Exception:
        return 0


def _pace():
    """Block until MIN_INTERVAL has elapsed since the last global call.
    Cross-process safe via an fcntl lock around a tiny timestamp file."""
    try:
        lf = open(PACE_LOCK, "w")
    except Exception:
        # Can't open the lock file — degrade to a plain sleep rather than burst.
        time.sleep(MIN_INTERVAL)
        return
    try:
        fcntl.flock(lf, fcntl.LOCK_EX)
        last = 0.0
        try:
            with open(PACE_PATH) as f:
                last = float(json.load(f).get("last", 0.0))
        except Exception:
            last = 0.0
        wait = MIN_INTERVAL - (time.time() - last)
        if wait > 0:
            time.sleep(min(wait, MAX_PACE_SLEEP))
        try:
            with open(PACE_PATH, "w") as f:
                json.dump({"last": time.time()}, f)
        except Exception:
            pass
    finally:
        try:
            fcntl.flock(lf, fcntl.LOCK_UN)
            lf.close()
        except Exception:
            pass


def _retry_after_from_exception(e):
    """Seconds Spotify asked us to wait, from a spotipy 429. 0 if it said nothing.

    Two places to look, because spotipy loses the header on the path that
    matters. When urllib3's retry machinery gives up it raises with empty
    headers ("Max Retries reached"), but the message it builds still carries
    the number:

        Your application has reached a rate/request limit.
        Retry will occur after: 11672 s

    Pure and defensive — a lockout is not the moment to raise a second
    exception out of the handler that is meant to record it.
    """
    try:
        headers = getattr(e, "headers", None) or {}
        raw = headers.get("Retry-After") or headers.get("retry-after")
        if raw:
            return max(0, int(str(raw).strip()))
    except Exception:
        pass
    try:
        m = re.search(r"retry\s+will\s+occur\s+after:\s*(\d+)",
                      str(getattr(e, "msg", "") or e), re.IGNORECASE)
        if m:
            return max(0, int(m.group(1)))
    except Exception:
        pass
    return 0


_RETRY_LOG_RE = re.compile(
    r"retry\s+will\s+occur\s+after:\s*(\d+)", re.IGNORECASE)


class _RetryAfterSniffer(logging.Handler):
    """Catch the Retry-After that spotipy logs and then throws away.

    THE ONLY PLACE THE REAL NUMBER SURVIVES on the path that matters, and
    the reason the gate spent months under-recording every lockout.

    With retries=0, urllib3 raises MaxRetryError on the first 429 and the
    RESPONSE — headers and all — is discarded. requests re-wraps it as a
    RetryError, and spotipy's handler for that raises

        SpotifyException(429, -1, f"{request.path_url}:\\n Max Retries",
                         reason=reason)

    with no `headers=` and no number anywhere in the text. So both branches
    of `_retry_after_from_exception` come back 0 and `record_429` falls to
    its 300s floor. Measured 2026-08-17: Spotify asked for 45,599s and the
    gate armed a 300s cooldown — it then declared itself clear inside five
    minutes and the next caller walked straight into another 429, re-arming
    the lockout. Precisely the failure the "record the LONGER" comment
    below was written to prevent, arriving by a different door.

    But one layer up, `spotipy.util.Retry.increment` still sees the live
    response and logs "Retry will occur after: N s" before calling super().
    So we listen for it. Attached as an extra handler, so spotipy's own
    logging is untouched.
    """

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.seconds = 0
        self.at = 0.0

    def emit(self, record):
        # A log handler that raises breaks the call it was observing, and this
        # one runs during a lockout — the worst possible moment for a second
        # exception. Nothing in here is allowed to fail loudly.
        try:
            m = _RETRY_LOG_RE.search(record.getMessage())
            if m:
                self.seconds = max(0, int(m.group(1)))
                self.at = time.time()
        except Exception:
            pass

    def recent(self, within=120.0):
        """The number, if it was logged just now. Time-boxed because the
        handler is process-global and a stale value from an earlier call
        must not be attributed to this one."""
        if self.seconds and (time.time() - self.at) <= within:
            return self.seconds
        return 0


_SNIFFER = _RetryAfterSniffer()
logging.getLogger("spotipy").addHandler(_SNIFFER)


def _synthetic_429(remaining, family="?"):
    """A 429 that looks real to existing handlers (they read http_status==429
    and Retry-After) but costs no network call. Names the family, because
    "the app is in cooldown" was never true and reading it in a log sent a
    session looking for an app-wide outage that did not exist."""
    return spotipy.SpotifyException(
        429, -1,
        f"/gate {family} in cooldown for {remaining}s (no call made)",
        headers={"Retry-After": str(remaining)},
    )


class GatedSpotify(spotipy.Spotify):
    """spotipy.Spotify whose every HTTP call is cooldown-guarded and paced."""

    def _internal_call(self, method, url, payload, params):
        family = spotify_health.endpoint_family(url)
        rem = cooldown_remaining(family)
        if rem > 0:
            raise _synthetic_429(rem, family)
        _pace()
        try:
            return super()._internal_call(method, url, payload, params)
        except spotipy.SpotifyException as e:
            if e.http_status == 429:
                # Three sources for how long to wait, and the LONGEST wins.
                # The exception itself is the least reliable: on the
                # RetryError path spotipy drops both the header and the
                # number (see _RetryAfterSniffer), which is why the log
                # sniffer exists and why it is read here.
                #
                # Why the longer: the numbers disagree, and under-waiting is
                # what re-arms a lockout. Measured 2026-08-07 — the real call
                # came back "Retry will occur after: 11672 s" while the probe
                # said 273, because the probe was asking a DIFFERENT endpoint
                # than the caller had just used. It now probes this call's own
                # family, so `from_probe` is finally an answer to the question
                # being asked; the 300s floor stays as a floor, not as the
                # usual outcome (it was the recorded value 192 times).
                from_call = _retry_after_from_exception(e)
                from_log = _SNIFFER.recent()
                ok, probed = spotify_health.probe(family)
                from_probe = probed if (not ok and probed) else 0
                spotify_health.record_429(
                    max(from_call, from_log, from_probe, 300), family)
            raise


def make_client(**kwargs):
    """Drop-in replacement for spotipy.Spotify(...). Defaults to the
    client-credentials flow and forces retries=0 so a 429 fails fast instead of
    spotipy internally sleeping on a huge Retry-After."""
    if "auth" not in kwargs and "auth_manager" not in kwargs:
        kwargs["auth_manager"] = SpotifyClientCredentials()
    kwargs.setdefault("retries", 0)
    kwargs.setdefault("status_retries", 0)
    return GatedSpotify(**kwargs)
