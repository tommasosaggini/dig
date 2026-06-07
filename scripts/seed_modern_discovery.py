#!/usr/bin/env python3
"""
DIG — Seed localized *modern*-genre discovery cells.

WHY THIS EXISTS
---------------
Discovery searches Spotify with `"{genre} year:{decade}"`. The genre token is
the ONLY geographic anchor — Spotify's `market` param controls licensing, not
artist nationality. So an *ethnic* genre name ("rebetiko", "csárdás", "guqin")
intrinsically returns that country's music, while a *generic* modern name
("indie rock") returns global music that merely happens to be licensed there.

Consequence: the only searches that cleanly attribute to a non-Western country
are its traditional ones. The pool therefore flattens every such country to a
folk stereotype — China is >50% classical, Mongolia 79% throat-singing,
Hungary's top genre is csárdás — while its living, contemporary scene (Uzbek
synth-pop, Vietnamese indie, Greek hip hop) is never collected.

The fix mirrors what already works for tradition: anchor modern-genre searches
with a *demonym* ("uzbek indie", "vietnamese hip hop", "greek trap"). Those
geo-constrain exactly like the ethnic terms do. This script generates those
cells per region and inserts them into `catalog_cells` so the normal discovery
loop will pick them up (and front-loads them via discovery_priorities).

USAGE
-----
  python3 scripts/seed_modern_discovery.py                 # dry-run (no writes)
  python3 scripts/seed_modern_discovery.py --apply         # write cells
  python3 scripts/seed_modern_discovery.py --regions "Central Asia,Vietnam"
  python3 scripts/seed_modern_discovery.py --apply --prioritize   # also set
                                                                  # discovery_priorities
"""
from __future__ import annotations

import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.db import get_conn, fetchall  # noqa: E402

# Valid discovery regions — MUST stay in sync with pipeline/discover.py REGIONS
# keys. We copy the names rather than import discover.py, because that module
# runs an entire discovery pass at import time (its pipeline is top-level, not
# under __main__). A dry-run must never trigger live Spotify searches.
VALID_REGIONS = {
    "UK", "Ireland", "France", "Germany", "Austria", "Switzerland", "Italy",
    "Spain", "Portugal", "Netherlands", "Belgium", "Nordic", "Baltic States",
    "Eastern Europe", "Ukraine", "Russia", "Balkans", "Greece", "Caucasus",
    "Turkey", "Iran", "Egypt", "North Africa", "Sudan", "Middle East",
    "Central Asia", "Afghanistan", "Japan", "South Korea", "China", "Hong Kong",
    "Taiwan", "Mongolia", "Tibet", "Thailand", "Vietnam", "Laos", "Myanmar",
    "Cambodia", "Indonesia", "Philippines", "Malaysia", "Singapore", "India",
    "Nepal", "South Asia", "West Africa", "Sahel", "Cape Verde", "Central Africa",
    "East Africa", "Horn of Africa", "Madagascar", "Southern Africa", "USA",
    "Canada", "Mexico", "Cuba", "Caribbean", "Central America", "Colombia",
    "Brazil", "Argentina", "Chile", "Peru", "South America", "Australia",
    "New Zealand", "Pacific Islands",
}


# ── Demonyms / locale anchors per region ────────────────────────────────────
# Each region maps to the search-term prefixes that geo-anchor a modern-genre
# query. Prefer demonyms ("uzbek") and well-known scene names. The non-Western
# regions are covered exhaustively — they are the ones being flattened.
REGION_LOCALES: dict[str, list[str]] = {
    # ── Eastern & Central Europe ──
    "Eastern Europe": ["hungarian", "polish", "czech", "slovak", "romanian", "bulgarian"],
    "Baltic States":  ["latvian", "lithuanian", "estonian"],
    "Ukraine":        ["ukrainian"],
    "Russia":         ["russian"],
    "Balkans":        ["serbian", "croatian", "bosnian", "slovenian", "macedonian", "albanian"],
    "Greece":         ["greek"],
    "Caucasus":       ["georgian", "armenian", "azerbaijani"],
    # ── MENA ──
    "Turkey":         ["turkish", "anatolian"],
    "Iran":           ["iranian", "persian"],
    "Egypt":          ["egyptian"],
    "North Africa":   ["moroccan", "algerian", "tunisian", "maghrebi"],
    "Sudan":          ["sudanese"],
    "Middle East":    ["lebanese", "iraqi", "saudi", "palestinian", "jordanian", "arabic"],
    # ── Central & South Asia ──
    "Central Asia":   ["uzbek", "kazakh", "kyrgyz", "tajik", "turkmen"],
    "Afghanistan":    ["afghan"],
    "India":          ["indian", "hindi", "tamil", "punjabi"],
    "Nepal":          ["nepali"],
    "South Asia":     ["pakistani", "bangladeshi", "sri lankan"],
    "Tibet":          ["tibetan"],
    # ── East Asia ──
    "Japan":          ["japanese"],
    "South Korea":    ["korean"],
    "China":          ["chinese", "mandarin"],
    "Hong Kong":      ["hong kong", "cantonese"],
    "Taiwan":         ["taiwanese"],
    "Mongolia":       ["mongolian"],
    # ── Southeast Asia ──
    "Thailand":       ["thai"],
    "Vietnam":        ["vietnamese"],
    "Laos":           ["lao"],
    "Myanmar":        ["burmese"],
    "Cambodia":       ["cambodian", "khmer"],
    "Indonesia":      ["indonesian"],
    "Philippines":    ["filipino", "pinoy"],
    "Malaysia":       ["malaysian"],
    "Singapore":      ["singaporean"],
    # ── Africa ──
    "West Africa":    ["nigerian", "ghanaian", "senegalese", "ivorian"],
    "Sahel":          ["malian", "mauritanian"],
    "Cape Verde":     ["cape verdean"],
    "Central Africa": ["congolese", "cameroonian", "angolan"],
    "East Africa":    ["kenyan", "tanzanian", "ugandan", "mozambican"],
    "Horn of Africa": ["ethiopian", "somali", "eritrean"],
    "Madagascar":     ["malagasy"],
    "Southern Africa":["south african", "zimbabwean", "zambian"],
    # ── Americas (non-US/UK) ──
    "Mexico":         ["mexican"],
    "Cuba":           ["cuban"],
    "Caribbean":      ["jamaican", "trinidadian", "haitian", "dominican"],
    "Central America":["guatemalan", "panamanian", "honduran", "salvadoran"],
    "Colombia":       ["colombian"],
    "Brazil":         ["brazilian"],
    "Argentina":      ["argentine"],
    "Chile":          ["chilean"],
    "Peru":           ["peruvian"],
    "South America":  ["venezuelan", "ecuadorian", "bolivian", "uruguayan", "paraguayan"],
    # ── Western Europe (lighter — already diverse, but fill modern gaps) ──
    "Portugal":       ["portuguese"],
    "Spain":          ["spanish"],
    "Italy":          ["italian"],
    "France":         ["french"],
    "Germany":        ["german"],
    "Nordic":         ["swedish", "norwegian", "finnish", "danish", "icelandic"],
    "Netherlands":    ["dutch"],
    "Belgium":        ["belgian"],
    "Ireland":        ["irish"],
    "Austria":        ["austrian"],
    "Switzerland":    ["swiss"],
    # ── Oceania ──
    "Australia":      ["australian"],
    "New Zealand":    ["new zealand", "maori"],
    "Pacific Islands":["fijian", "samoan", "tongan"],
}

# ── Contemporary genres worth a local scene ─────────────────────────────────
# Kept deliberately broad across the modern spectrum. Each becomes "{demonym}
# {genre}" — e.g. "uzbek hip hop", "vietnamese indie rock", "greek techno".
MODERN_GENRES = [
    "hip hop", "rap", "trap", "drill",
    "indie", "indie rock", "indie pop", "alternative rock",
    "pop", "synth pop", "new wave", "electropop",
    "rock", "punk", "post-punk", "garage rock", "psychedelic rock",
    "electronic", "techno", "house", "experimental", "ambient",
    "r&b", "soul", "funk", "jazz", "neo-soul",
    "metal", "hardcore",
    "dream pop", "shoegaze", "lo-fi", "bedroom pop", "disco",
]

# Modern eras only — the whole point is to find living scenes.
DECADES = ["2000s", "2010s", "2020s"]


def build_cells(regions_filter: set[str] | None) -> list[tuple[str, str, str]]:
    """Return list of (region, genre, decade) cells to seed."""
    cells = []
    for region, locales in REGION_LOCALES.items():
        if regions_filter and region not in regions_filter:
            continue
        if region not in VALID_REGIONS:
            # Region not in discovery's market map — discovery would skip it.
            print(f"  ⚠ skipping {region!r}: not in discover.REGIONS market map")
            continue
        for locale in locales:
            for genre in MODERN_GENRES:
                term = f"{locale} {genre}"
                for decade in DECADES:
                    cells.append((region, term, decade))
    return cells


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually write cells (default: dry-run)")
    ap.add_argument("--regions", default="", help="comma-separated region filter")
    ap.add_argument("--prioritize", action="store_true",
                    help="also register the seeded genres in discovery_priorities.missing_genres")
    args = ap.parse_args()

    regions_filter = {r.strip() for r in args.regions.split(",") if r.strip()} or None
    cells = build_cells(regions_filter)

    # Which cell_ids already exist?
    existing = set()
    if cells:
        rows = fetchall(
            "SELECT cell_id FROM catalog_cells WHERE region = ANY(%s)",
            (sorted({c[0] for c in cells}),),
        )
        existing = {r["cell_id"] for r in rows}

    new_cells = []
    for region, genre, decade in cells:
        cell_id = f"{region}|{genre}|{decade}"
        if cell_id not in existing:
            new_cells.append((cell_id, region, genre, decade))

    # ── Report ──
    by_region: dict[str, int] = {}
    for _, region, _, _ in new_cells:
        by_region[region] = by_region.get(region, 0) + 1

    print(f"\nRegions seeded:     {len(by_region)}")
    print(f"Locales total:      {sum(len(v) for r, v in REGION_LOCALES.items() if not regions_filter or r in regions_filter)}")
    print(f"Modern genres:      {len(MODERN_GENRES)}  ×  decades: {len(DECADES)}")
    print(f"Candidate cells:    {len(cells)}")
    print(f"Already present:    {len(cells) - len(new_cells)}")
    print(f"NEW cells to write: {len(new_cells)}\n")

    for region in sorted(by_region):
        print(f"  {region:18s} +{by_region[region]} cells")

    # Sample of the actual search terms that will run.
    sample_terms = sorted({g for _, _, g, _ in new_cells})
    print(f"\nSample modern terms ({min(20, len(sample_terms))} of {len(sample_terms)}):")
    for t in sample_terms[:20]:
        print(f"    \"{t} year:2015-2024\"")

    if not args.apply:
        print("\n[DRY-RUN] No writes. Re-run with --apply to insert these cells.")
        return

    # ── Write ──
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO catalog_cells (cell_id, region, genre, decade, explored, fetched) "
                "VALUES (%s, %s, %s, %s, 0, 0) ON CONFLICT (cell_id) DO NOTHING",
                new_cells,
            )
        conn.commit()
        print(f"\n✓ Inserted {len(new_cells)} cells into catalog_cells.")
    finally:
        conn.close()

    if args.prioritize:
        from lib.db import get_meta, set_meta  # type: ignore
        genres = sorted({g for _, _, g, _ in new_cells})
        prior = get_meta("discovery_priorities") or {}
        missing = list(dict.fromkeys((prior.get("missing_genres") or []) + genres))
        prior["missing_genres"] = missing
        set_meta("discovery_priorities", prior)
        print(f"✓ Registered {len(genres)} genres in discovery_priorities.missing_genres "
              f"(now {len(missing)} total).")


if __name__ == "__main__":
    main()
