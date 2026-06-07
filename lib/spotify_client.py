"""DIG — shared Spotify helpers for the discovery pipeline.

Currently holds track extraction (the Spotify search/track object -> DIG track
dict conversion that discover.py and discover_artists.py both performed
identically). The gated/budgeted API-call wrapper is a natural future addition
here so every script shares one 429/quota policy.
"""


def extract_track(t, query="", decade=""):
    """Convert a Spotify search/track object into a DIG track dict.

    `query` records the search/phase that found the track. `decade` may be
    passed explicitly; otherwise it's derived from the album release year.
    Genres are intentionally left empty — assigning them from the free-form
    query string produced polluted entries (e.g. "hindustani classical raag
    vocal sitar sarod 1970s 1980s"); label_discovery.py assigns 1-3 canonical
    genres later via Claude against a controlled vocabulary.
    """
    artists = t.get("artists", [])
    artist = ", ".join(a["name"] for a in artists)
    artist_ids = [a["id"] for a in artists if a.get("id")]
    album = t.get("album", {})
    release_date = album.get("release_date", "")
    year = release_date[:4] if len(release_date) >= 4 else ""
    if not decade and year:
        decade = year[:3] + "0s"  # "1975" -> "1970s"
    track = {
        "name": t.get("name", ""),
        "artist": artist,
        "artist_ids": artist_ids,
        "id": t["id"],
        "album": album.get("name", ""),
        "popularity": t.get("popularity", 0),
        "query": query,
        "source": "spotify",
    }
    if decade:
        track["decade"] = decade
    if year:
        track["year"] = year
    return track
