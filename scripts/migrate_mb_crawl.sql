-- mb_crawl_state: BFS frontier tracking for the genre-seed crawler.
-- Populated by scripts/crawl_genre_seeds.py — one row per MBID we've
-- visited so re-runs don't re-fetch the same artist's relationships.
-- Separate from mb_artists because that table is "artists we know
-- exist"; this table is "artists we've already expanded the graph
-- around." Many MBIDs appear in mb_artists without ever being a crawl
-- frontier (they're discovered as a relationship target but never
-- expanded outward).
CREATE TABLE IF NOT EXISTS mb_crawl_state (
    mbid           TEXT PRIMARY KEY,
    crawled_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    depth          INTEGER NOT NULL,   -- 0 = seed, 1 = 1st hop, …
    seed_genre     TEXT,                -- genre that originated this branch
    seed_qid       TEXT,                -- Wikidata QID of that genre
    rels_found     INTEGER DEFAULT 0,   -- how many neighbors we discovered
    new_artists    INTEGER DEFAULT 0,   -- of those, how many were new to mb_artists
    error          TEXT
);

CREATE INDEX IF NOT EXISTS idx_mb_crawl_seed_genre
  ON mb_crawl_state (seed_genre);
CREATE INDEX IF NOT EXISTS idx_mb_crawl_uncrawled_frontier
  ON mb_crawl_state (depth, crawled_at);
