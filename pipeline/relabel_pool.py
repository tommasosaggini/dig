#!/usr/bin/env python3
"""
Relabel the discovery pool via the Batch API.

The labels drive the recommendation engine — a track's energy/mood/texture/feel
is what the taste profile matches against — so systematic label error is
systematic recommendation error. Measured across 30k tracks, k-pop is labelled
"euphoric" 3.7x more than its runner-up where no mood dominates overall: the
labeller was reading reputations, not records.

This pass fixes what can be fixed cheaply and at full coverage:
  * the crawler's `query` string (noise — a NewJeans song can arrive via
    "catalog:qawwali 1960s-1980s") is no longer in the prompt
  * an explicit instruction not to reach for the genre's default mood
  * where `tracks.audio_features` exists, the actual measurements are included
    and the model is told to weight them above genre priors

That last point makes this re-runnable: as pipeline/audio_analyze.py works
through the pool, running this again upgrades those tracks from text-only
guesses to audio-grounded labels without re-downloading anything.

Batch API = 50% off and it is not latency-sensitive; ~42k tracks costs about
$9 on Haiku 4.5.

    python3 pipeline/relabel_pool.py submit [--limit N] [--only-analyzed]
    python3 pipeline/relabel_pool.py collect <batch_id> [--write]
"""
import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()
from lib import audio_features
from lib.db import execute, fetchall
from pipeline.ig_relabel_audio import ENERGY, FEEL, MOOD, TEXTURE, USE_CASE

MODEL = os.environ.get("DIG_POOL_LABEL_MODEL", "claude-haiku-4-5")
PER_CALL = 25            # tracks per request; keeps output well under max_tokens
FIELDS = ("energy", "mood", "texture", "feel", "use_case")

HEADER = f"""Label each track with these fields, picking ONLY from the exact values listed.

CRITICAL — label THIS RECORDING, not its genre's reputation.
Most tracks in a genre are not the genre's stereotype. A k-pop single can be
wistful and hazy rather than euphoric; a metal track can be tender; a house
record can be melancholic. Where a track line includes MEASURED FROM THE AUDIO,
those numbers come from the actual recording — weight them above anything you
assume from the artist or genre. Where there is no measurement and you do not
know the specific song, only then fall back to what is typical for the artist.
Reaching for the genre's default mood is the single most common way these
labels go wrong.

- energy: {" | ".join(ENERGY)}
- mood (exactly one): {" | ".join(MOOD)}
- texture (1-2, comma separated): {" | ".join(TEXTURE)}
- feel (exactly one): {" | ".join(FEEL)}
- use_case (exactly one): {" | ".join(USE_CASE)}

Return ONLY valid JSON, no markdown:
{{"track_id": {{"energy":"...","mood":"...","texture":"...","feel":"...","use_case":"..."}}}}

Tracks:
"""


def track_line(r):
    line = f"{r['id']} | {r['artist']} — {r['name']}"
    if r.get("album"):
        line += f" [{r['album']}]"
    if r.get("genres"):
        line += f" (genres: {', '.join(list(r['genres'])[:3])})"
    feats = r.get("audio_features")
    if feats:
        if isinstance(feats, str):
            feats = json.loads(feats)
        line += f"\n    MEASURED FROM THE AUDIO: {audio_features.describe(feats)}"
    return line


def submit(limit, only_analyzed):
    # Never downgrade a track that was already labelled from its audio unless
    # this pass also has measurements for it. Without this guard the text pass
    # silently overwrote the audio-grounded labels with weaker guesses — the
    # exact regression this whole exercise exists to remove.
    guard = "(audio_labeled_at IS NULL OR audio_features IS NOT NULL)"
    where = ("WHERE audio_features IS NOT NULL" if only_analyzed
             else f"WHERE {guard}")
    rows = fetchall(f"""
        SELECT id, name, artist, album, genres, audio_features
        FROM tracks {where}
        ORDER BY (audio_features IS NOT NULL) DESC, quality_score DESC NULLS LAST
        LIMIT %s
    """, (limit,))
    if not rows:
        print("no tracks matched.")
        return

    import anthropic
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request

    requests, analyzed = [], 0
    for i in range(0, len(rows), PER_CALL):
        chunk = rows[i:i + PER_CALL]
        analyzed += sum(1 for r in chunk if r.get("audio_features"))
        requests.append(Request(
            custom_id=f"b{i // PER_CALL}",
            params=MessageCreateParamsNonStreaming(
                model=MODEL,
                max_tokens=400 + 200 * len(chunk),
                messages=[{"role": "user",
                           "content": HEADER + "\n".join(track_line(r) for r in chunk)}],
            ),
        ))

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    batch = client.messages.batches.create(requests=requests)
    print(f"submitted {len(rows)} tracks in {len(requests)} requests "
          f"({analyzed} with audio measurements) on {MODEL}")
    print(f"batch id: {batch.id}")
    print(f"collect with:  python3 pipeline/relabel_pool.py collect {batch.id} --write")


def collect(batch_id, write):
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    batch = client.messages.batches.retrieve(batch_id)
    print(f"status: {batch.processing_status}  {batch.request_counts}")
    if batch.processing_status != "ended":
        print("not finished yet — re-run later.")
        return

    rows, failed = [], 0
    for result in client.messages.batches.results(batch_id):
        if result.result.type != "succeeded":
            failed += 1
            continue
        text = "".join(b.text for b in result.result.message.content
                       if getattr(b, "type", "") == "text").strip()
        if text.startswith("```"):
            text = text.split("```")[1].lstrip("json").strip()
        try:
            out = json.loads(text)
        except json.JSONDecodeError:
            failed += 1
            continue
        for tid, lab in out.items():
            if all(lab.get(f) for f in FIELDS):
                rows.append(tuple(lab[f] for f in FIELDS) + (tid,))

    if write and rows:
        # One connection, one statement. lib.db.execute() opens a fresh
        # connection per call, which is fine for a handful of writes but not
        # for 42k of them across an SSH tunnel — that ran at roughly 30 rows a
        # minute. Updating from a VALUES list finishes in seconds instead.
        from psycopg2.extras import execute_values
        from lib.db import get_conn
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, """
                    UPDATE tracks t SET
                        label_energy   = v.energy,
                        label_mood     = v.mood,
                        label_texture  = v.texture,
                        label_feel     = v.feel,
                        label_use_case = v.use_case
                    FROM (VALUES %s) AS v(energy, mood, texture, feel, use_case, id)
                    WHERE t.id = v.id
                """, rows, page_size=1000)
            conn.commit()
        finally:
            conn.close()

    print(f"{len(rows)} track labels {'written' if write else 'parsed (dry run)'}; "
          f"{failed} request(s) unusable")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("submit")
    s.add_argument("--limit", type=int, default=100000)
    s.add_argument("--only-analyzed", action="store_true",
                   help="only tracks that already have audio measurements")
    c = sub.add_parser("collect")
    c.add_argument("batch_id")
    c.add_argument("--write", action="store_true")
    args = ap.parse_args()

    if args.cmd == "submit":
        submit(args.limit, args.only_analyzed)
    else:
        collect(args.batch_id, args.write)


if __name__ == "__main__":
    main()
