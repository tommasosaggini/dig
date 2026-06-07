"""DIG — canonical "artist - track" dedup key.

One normaliser shared by every heard/ledger dedup path (ai_recommend's V1/V2/
journey recommenders and pool_search) so they can never silently diverge.
Collapses different pressings of the same song to one key: strips a trailing
parenthetical/bracket suffix ("(Remix)", "[Live]") and a trailing
" - Remaster/Remix/Live/Version/Edit/Mix ...".
"""
import re

_PAREN_SUFFIX = re.compile(r"\s*[\(\[].*?[\)\]]\s*$")
_DASH_SUFFIX = re.compile(r"\s+-\s+(remaster|remix|live|version|edit|mix).*$")


def normalize(artist, track):
    """Return the lowercased "primary-artist - cleaned-track" dedup key."""
    a = (artist or "").split(",")[0].strip().lower()
    t = (track or "").strip().lower()
    t = _PAREN_SUFFIX.sub("", t)
    t = _DASH_SUFFIX.sub("", t)
    return f"{a} - {t}".strip()
