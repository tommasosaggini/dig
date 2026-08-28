"""Every rung of the origin ladder must remember what IT has already asked.

WHAT THIS GUARDS

resolve_origin.py walks six rungs, cheapest and most exact first, and it kept
its progress in one column — `origin_checked_at`. One flag cannot answer six
independent questions, and the three network stages each read it differently:

    stage_wikidata      WHERE ... AND origin_checked_at IS NULL   honoured it
    stage_bandcamp      WHERE ... AND source='bandcamp'           ignored it
    stage_musicbrainz   WHERE ... AND artist_ids IS NOT NULL      ignored it

The two that ignored it had no cursor at all: `LIMIT 150`, no ORDER BY, no
"not yet asked" filter, so every run re-read the same rows. Measured
2026-08-28, stage_bandcamp's entire population:

    total 2,127   already asked 2,122

Four runs a day, 150 rows a run, advancing only by however many happened to
resolve — the rest came back next run and said no again. The log looked busy
and the trusted share sat at 87.0%.

WHY NOT JUST MAKE THEM ALL HONOUR origin_checked_at

Because that trades this defect for a worse one. Whichever stage touched a row
first would mark it done for every other stage, so the cheap rungs would strip
the pool before the expensive ones saw it — and the measured yields say that is
backwards: Wikidata-by-name lands 23%, the Bandcamp artist page 58%. The cheap
rung running more often would quietly retire the pool's best remaining source.

So each stage records its own attempt, and asks only for rows it has not asked.

    python3 tests/test_origin_ladder_has_a_cursor.py
    pytest tests/test_origin_ladder_has_a_cursor.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

SRC = open(os.path.join(ROOT, "scripts", "resolve_origin.py"),
           encoding="utf-8").read()

# The rungs that spend network requests. stage_offline is excluded on purpose:
# it is a join against our own mb_artists table, costs nothing, and re-running
# it is free — a cursor there would be bookkeeping for its own sake.
NETWORK_STAGES = ("wikidata", "bandcamp", "musicbrainz")


def _body(stage):
    """The source of one stage function, up to the next top-level def."""
    m = re.search(rf"^def stage_{stage}\(.*?(?=^def |\Z)", SRC,
                  re.M | re.S)
    assert m, f"stage_{stage} is gone — did it get renamed?"
    return m.group(0)


def test_every_network_stage_filters_on_its_own_cursor():
    """A stage must not ask for rows it has already asked about."""
    missing = [s for s in NETWORK_STAGES if "NOT_YET_TRIED" not in _body(s)]
    assert not missing, (
        "these stages re-read the same rows every run, for ever: "
        + ", ".join(missing))


def test_every_network_stage_passes_its_own_name_to_that_filter():
    """Sharing one name would rebuild the single-flag bug in a new column."""
    for stage in NETWORK_STAGES:
        body = _body(stage)
        assert f'"{stage}"' in body, (
            f"stage_{stage} uses the cursor but never names itself — it is "
            f"either filtering on another stage's progress or on nothing")


def test_the_cursor_placeholder_comes_before_the_limit():
    """psycopg2 binds by position, so the order is not cosmetic.

    NOT_YET_TRIED carries a `%s` and every stage query ends in `LIMIT %s`. Pass
    them the wrong way round and the stage filters on the row limit and asks
    Postgres for `'wikidata'` rows — which is not an error, just an empty
    ladder that reports success.
    """
    for stage in NETWORK_STAGES:
        body = _body(stage)
        q = re.search(r"fetchall\(f?\"\"\"(.*?)\"\"\"", body, re.S)
        assert q, f"stage_{stage}: could not find its query"
        sql = q.group(1)
        assert "{NOT_YET_TRIED}" in sql, stage
        assert sql.index("{NOT_YET_TRIED}") < sql.rindex("LIMIT %s"), (
            f"stage_{stage} puts its cursor placeholder after LIMIT, so the "
            f"two arguments bind to each other's placeholders")


def test_every_network_stage_records_that_it_asked():
    """Filtering without recording is a cursor that never advances."""
    for stage in NETWORK_STAGES:
        body = _body(stage)
        assert f'_mark_checked(checked, "{stage}")' in body, (
            f"stage_{stage} filters on its cursor but never writes to it, so "
            f"it will re-ask the same rows for ever — the original bug")


def test_marking_without_a_stage_still_works():
    """The plain form stays, for callers that mean "settled", not "this rung"."""
    m = re.search(r"def _mark_checked\(track_ids, stage=None\)", SRC)
    assert m, "_mark_checked lost its optional stage argument"
    assert "array_append" in SRC, "the per-stage branch is gone"


def test_the_migration_ships_with_the_code():
    """A stage cursor that has no column to write to fails at 03:00, not now."""
    path = os.path.join(ROOT, "scripts", "migrate_origin_stage_cursor.sql")
    assert os.path.exists(path), "migrate_origin_stage_cursor.sql is missing"
    sql = open(path, encoding="utf-8").read()
    assert "origin_stages_tried" in sql
    assert "ADD COLUMN IF NOT EXISTS" in sql, (
        "the migration must be re-runnable — it is applied by hand on prod")


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"ok   {name}")
        except AssertionError as e:
            print(f"FAIL {name}: {e}")
            failed += 1
    print("all origin-ladder cursor checks passed" if not failed
          else f"\n{failed} failed")
    sys.exit(1 if failed else 0)
