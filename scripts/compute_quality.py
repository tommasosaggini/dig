#!/usr/bin/env python3
"""
DIG — Compute track quality scores from user engagement.

Scans user_history for all plays with played_pct/saves/dislikes and
accumulates a quality score per track. Tracks that get saved or deeply
listened to rise; tracks that get instant-skipped or disliked sink.

The score is stored on the tracks table as quality_score + quality_plays.
Runs idempotently — recomputes from scratch each time.

Usage:  python3 scripts/compute_quality.py
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.env import load_env
load_env()

from lib.db import get_conn, fetchall


def main():
    # Pull all engagement data
    rows = fetchall("""
        SELECT track_id, status, played_pct
        FROM user_history
        WHERE track_id IS NOT NULL
    """)
    print(f"Processing {len(rows)} history entries...")

    # Accumulate scores per track
    scores = {}  # track_id → {score, plays}
    for r in rows:
        tid = r["track_id"]
        if tid not in scores:
            scores[tid] = {"score": 0.0, "plays": 0}

        pct = r.get("played_pct")
        status = r.get("status")

        # Status-based scoring (strongest signals)
        if status == "saved":
            scores[tid]["score"] += 5.0
        elif status == "disliked":
            scores[tid]["score"] -= 5.0

        # Played_pct-based scoring (continuous signal)
        if pct is not None:
            if pct >= 80:
                scores[tid]["score"] += 2.0
            elif pct >= 50:
                scores[tid]["score"] += 0.5
            elif pct >= 20:
                pass  # neutral
            elif pct >= 10:
                scores[tid]["score"] -= 0.5
            else:
                scores[tid]["score"] -= 2.0

        scores[tid]["plays"] += 1

    print(f"Scored {len(scores)} distinct tracks")

    # Distribution
    positive = sum(1 for s in scores.values() if s["score"] > 0)
    negative = sum(1 for s in scores.values() if s["score"] < 0)
    neutral = sum(1 for s in scores.values() if s["score"] == 0)
    print(f"  Positive: {positive}, Neutral: {neutral}, Negative: {negative}")

    # Write to DB
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Reset all scores first
            cur.execute("UPDATE tracks SET quality_score = 0, quality_plays = 0")
            # Apply computed scores
            BATCH = 200
            items = list(scores.items())
            written = 0
            for i in range(0, len(items), BATCH):
                chunk = items[i:i + BATCH]
                for tid, data in chunk:
                    cur.execute(
                        "UPDATE tracks SET quality_score = %s, quality_plays = %s WHERE id = %s",
                        (round(data["score"], 2), data["plays"], tid),
                    )
                written += len(chunk)
                if written % 1000 == 0:
                    print(f"  wrote {written}/{len(items)}")
        conn.commit()
        print(f"DONE — scored {len(scores)} tracks.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
