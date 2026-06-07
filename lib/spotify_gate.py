"""
DIG — Global Spotify call gate (the single "drip-lane").

Spotify Extended Quota is NOT available to us, so Development Mode is permanent
and its quota is tiny + app-wide. The 24h lockouts are caused by BURSTING, not
by total volume — so the whole strategy is: never burst, just trickle.

Every Spotify API call across every script flows through one `GatedSpotify`
client. Its `_internal_call` (spotipy's single HTTP chokepoint) does, in order:

  1. COOLDOWN — if the app is in a recorded 429 cooldown, raise a synthetic 429
     immediately (no network call). Existing per-script 429 handlers then abort
     cleanly, exactly as they do for a real 429.
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
import os
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


def cooldown_remaining():
    """Seconds left on a recorded 429 cooldown (0 if clear). Cheap file read —
    never probes Spotify."""
    try:
        with open(spotify_health.CACHE_PATH) as f:
            c = json.load(f)
        rem = int(c.get("cooldown_until", 0)) - int(time.time())
        return rem if rem > 0 else 0
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


def _synthetic_429(remaining):
    """A 429 that looks real to existing handlers (they read http_status==429
    and Retry-After) but costs no network call."""
    return spotipy.SpotifyException(
        429, -1,
        f"/gate app in cooldown for {remaining}s (no call made)",
        headers={"Retry-After": str(remaining)},
    )


class GatedSpotify(spotipy.Spotify):
    """spotipy.Spotify whose every HTTP call is cooldown-guarded and paced."""

    def _internal_call(self, method, url, payload, params):
        rem = cooldown_remaining()
        if rem > 0:
            raise _synthetic_429(rem)
        _pace()
        try:
            return super()._internal_call(method, url, payload, params)
        except spotipy.SpotifyException as e:
            if e.http_status == 429:
                # spotipy's 429 exception drops the Retry-After header (urllib3's
                # retry swallows the response → empty headers, "Max Retries").
                # The raw-urllib probe reads the real value reliably. Just ONE
                # probe per lockout: once the cooldown is recorded, later calls
                # raise a synthetic 429 and never reach here.
                ok, retry = spotify_health.probe()
                spotify_health.record_429(retry if (not ok and retry) else 300)
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
