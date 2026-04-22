"""
DIG — Cell-bounded ingest accounting.

A single place where all three ingest paths (discover.py, discover_artists.py,
discover_youtube.py) enforce the breadth-first principle: each (region, genre,
decade) cell caps out at CELL_TARGET tracks; within a cell, no more than
CELL_PER_ARTIST tracks per artist and CELL_PER_ALBUM per album; across the
whole pool, no more than ARTIST_POOL_CAP tracks per artist.

Cells are emergent — a track defines the cell it lands in. No a priori grid.
The catalog_cells table is updated lazily (via _upsert_track) so queries stay
consistent with reality without needing a separate reconciliation step.
"""

# Caps — deliberately conservative, tuneable from here.
CELL_TARGET     = 25    # tracks per (region, genre, decade)
CELL_PER_ARTIST = 2     # tracks per artist per cell
CELL_PER_ALBUM  = 1     # tracks per album per cell
ARTIST_POOL_CAP = 5     # tracks per artist across the whole pool


def derive_genre(track: dict) -> str:
    """Pick a single representative genre for a track.

    Preference order:
      1. First entry in the Spotify artist genres list (most canonical)
      2. Catalog query genre, if the query looks like "catalog:<genre> ..."
      3. "unknown"
    """
    genres = track.get("genres") or []
    if genres:
        g = str(genres[0]).strip()
        if g:
            return g

    q = track.get("query") or ""
    if q.startswith("catalog:"):
        tail = q[len("catalog:"):].strip()
        # Strip trailing year:XXXX filter from the query
        if " year:" in tail:
            tail = tail.split(" year:")[0].strip()
        if tail:
            return tail

    return "unknown"


def cell_coord(track: dict, region_override: str | None = None) -> tuple[str, str, str]:
    """Return (region, genre, decade) for this track.

    Prefers origin_region (authoritative artist nationality) over region
    (discovery bucket / search market).
    """
    region = (
        region_override
        or track.get("origin_region")
        or track.get("region")
        or "Unknown"
    )
    genre  = derive_genre(track)
    decade = (track.get("decade") or "").strip() or "unknown"
    return (region, genre, decade)


class CellAccountant:
    """Track per-cell, per-artist, and per-album counts during an ingest run.

    Build from the current pool at the start of a run (`CellAccountant.from_pool`),
    then for every candidate new track call `can_ingest(track)` — if True,
    append to the pool and call `register(track)` to update the counts.
    """

    def __init__(self) -> None:
        self.artist_pool: dict[str, int] = {}
        self.cell_total:  dict[tuple, int] = {}
        self.cell_artist: dict[tuple, dict[str, int]] = {}
        self.cell_album:  dict[tuple, dict[str, int]] = {}
        # Reason counters for diagnostic logging
        self.rejections: dict[str, int] = {
            "artist_pool_cap": 0,
            "cell_target": 0,
            "cell_per_artist": 0,
            "cell_per_album": 0,
        }

    @classmethod
    def from_pool(cls, iter_tracks) -> "CellAccountant":
        """Build from an iterable of track dicts (e.g. flatten of load_discovery())."""
        acc = cls()
        for t in iter_tracks:
            acc._register_existing(t)
        return acc

    def _register_existing(self, t: dict) -> None:
        ak = (t.get("artist") or "").lower()
        if ak:
            self.artist_pool[ak] = self.artist_pool.get(ak, 0) + 1
        cell = cell_coord(t)
        self.cell_total[cell] = self.cell_total.get(cell, 0) + 1
        if ak:
            ca = self.cell_artist.setdefault(cell, {})
            ca[ak] = ca.get(ak, 0) + 1
        album = (t.get("album") or "").lower()
        if album:
            cb = self.cell_album.setdefault(cell, {})
            cb[album] = cb.get(album, 0) + 1

    def can_ingest(self, t: dict, region_override: str | None = None) -> tuple[bool, str | None]:
        """Check whether this track can be added without exceeding any cap."""
        ak = (t.get("artist") or "").lower()
        if ak and self.artist_pool.get(ak, 0) >= ARTIST_POOL_CAP:
            self.rejections["artist_pool_cap"] += 1
            return False, "artist_pool_cap"

        cell = cell_coord(t, region_override)

        if self.cell_total.get(cell, 0) >= CELL_TARGET:
            self.rejections["cell_target"] += 1
            return False, "cell_target"

        if ak and self.cell_artist.get(cell, {}).get(ak, 0) >= CELL_PER_ARTIST:
            self.rejections["cell_per_artist"] += 1
            return False, "cell_per_artist"

        album = (t.get("album") or "").lower()
        if album and self.cell_album.get(cell, {}).get(album, 0) >= CELL_PER_ALBUM:
            self.rejections["cell_per_album"] += 1
            return False, "cell_per_album"

        return True, None

    def register(self, t: dict, region_override: str | None = None) -> None:
        """Record that this track has been ingested. Call after can_ingest()=True."""
        ak = (t.get("artist") or "").lower()
        if ak:
            self.artist_pool[ak] = self.artist_pool.get(ak, 0) + 1
        cell = cell_coord(t, region_override)
        self.cell_total[cell] = self.cell_total.get(cell, 0) + 1
        if ak:
            ca = self.cell_artist.setdefault(cell, {})
            ca[ak] = ca.get(ak, 0) + 1
        album = (t.get("album") or "").lower()
        if album:
            cb = self.cell_album.setdefault(cell, {})
            cb[album] = cb.get(album, 0) + 1

    def rejection_summary(self) -> str:
        """One-line summary suitable for printing after a batch."""
        parts = [f"{k}={v}" for k, v in self.rejections.items() if v]
        return ", ".join(parts) if parts else "none"
