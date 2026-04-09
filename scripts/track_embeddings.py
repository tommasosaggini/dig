#!/usr/bin/env python3
"""
DIG — Track-level 2D embeddings.

Combines genre embedding, mood, energy, texture, region, and year into a
composite feature vector per track, then projects to 2D via t-SNE.

Output: track_map.json — {track_id: [x, y]} for all tracks.
The UI loads this for the unified Explore visualization.

Run after label_discovery.py in the cron pipeline (or on-demand).
"""

import json
import math
import os
import sys

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR = ROOT


def main():
    print("\n🗺️  DIG — TRACK EMBEDDINGS\n")

    # ── Load data ──
    discovery_path = os.path.join(DIR, "discovery.json")
    genre_map_path = os.path.join(DIR, "genre_map.json")

    if not os.path.exists(discovery_path):
        print("  No discovery.json found.")
        sys.exit(1)

    with open(discovery_path) as f:
        discovery = json.load(f)

    genre_map = None
    if os.path.exists(genre_map_path):
        with open(genre_map_path) as f:
            genre_map = json.load(f)
        print(f"  Genre map: {genre_map.get('genre_count', 0)} genres")
    else:
        print("  No genre_map.json — genre dimension will use hashing")

    # ── Collect all tracks ──
    tracks = []
    for region, track_list in discovery.items():
        for t in track_list:
            t["_region"] = region
            tracks.append(t)

    print(f"  Total tracks: {len(tracks)}")
    if not tracks:
        sys.exit(0)

    # ── Build region encoding ──
    all_regions = sorted(set(t["_region"] for t in tracks))
    region_to_idx = {r: i for i, r in enumerate(all_regions)}
    n_regions = len(all_regions)
    print(f"  Regions: {n_regions}")

    # ── Energy encoding ──
    energy_map = {"very low": 0.0, "low": 0.25, "moderate": 0.5, "high": 0.75, "very high": 1.0}

    # ── Mood hue encoding (same as UI vibeColor) ──
    import re
    def mood_to_vec(mood_str):
        """Convert mood string to a 2D vector on a circle (like hue)."""
        mood = (mood_str or "").lower()
        # Map mood to angle (radians), same categories as UI
        angle = 3.14  # default: neutral
        if re.search(r"seren|calm|meditat|peace|tranquil|gentle", mood):
            angle = 3.67  # ~210°
        elif re.search(r"warm|nostalg|golden|amber|cozy", mood):
            angle = 0.61  # ~35°
        elif re.search(r"dark|heavy|aggress|intense|raw|grit", mood):
            angle = 5.76  # ~330°
        elif re.search(r"bright|joy|euphor|uplift|happy|celebr", mood):
            angle = 1.40  # ~80°
        elif re.search(r"myster|ethereal|dream|haunt|eerie", mood):
            angle = 4.71  # ~270°
        elif re.search(r"bitter|melan|sad|sorrow|longing", mood):
            angle = 4.19  # ~240°
        elif re.search(r"playful|funky|groov|bounce", mood):
            angle = 0.87  # ~50°
        elif re.search(r"rebel|punk|chaos|frenet", mood):
            angle = 0.0  # ~0°
        elif re.search(r"spirit|sacred|devot|reveren", mood):
            angle = 5.06  # ~290°
        return [math.cos(angle), math.sin(angle)]

    # ── Texture encoding (simple keyword hashing to a few dimensions) ──
    TEXTURE_KEYWORDS = [
        "warm", "analog", "crisp", "digital", "hazy", "lo-fi", "lush",
        "orchestral", "raw", "distorted", "shimmer", "synth", "acoustic",
        "electric", "heavy", "light", "deep", "bright", "smooth", "gritty",
        "wooden", "metallic", "ethereal", "percussive", "vocal", "ambient",
    ]
    def texture_to_vec(texture_str):
        """Convert texture string to a sparse feature vector."""
        tex = (texture_str or "").lower()
        return [1.0 if kw in tex else 0.0 for kw in TEXTURE_KEYWORDS]

    # ── Build feature vectors ──
    print("  Building feature vectors...")

    # Weights for each dimension (controls influence on final layout)
    # Genre coords: strongest signal (2D, scaled)
    # Mood: important for vibe clustering
    # Energy: important
    # Region: moderate (we want cross-region genre clusters)
    # Year: weak (year has its own view)
    # Texture: subtle

    W_GENRE = 3.0      # genre embedding position (2 dims)
    W_MOOD = 2.0       # mood circle position (2 dims)
    W_ENERGY = 1.5     # energy level (1 dim)
    W_REGION = 0.8     # region one-hot (many dims, but sparse)
    W_YEAR = 0.5       # year normalized (1 dim)
    W_TEXTURE = 0.6    # texture keywords (many dims)

    feature_vecs = []
    track_ids = []

    for t in tracks:
        vec = []

        # 1. Genre embedding coords (2D from genre_map)
        genre = ""
        if t.get("genres"):
            genre = t["genres"][0].lower()
        else:
            q = t.get("query", "")
            if q.startswith("catalog:"):
                genre = q[8:].split(" year:")[0].strip().lower()
            elif q.startswith("hint:"):
                genre = q[5:].strip().lower()
            elif not q.startswith("random:"):
                genre = q.strip().lower()

        if genre_map and genre in genre_map.get("coords", {}):
            gc = genre_map["coords"][genre]
            vec.extend([gc[0] / 100.0 * W_GENRE, gc[1] / 100.0 * W_GENRE])
        elif genre_map:
            # Try to find closest genre
            best_match = None
            for g in genre_map.get("coords", {}):
                if genre in g or g in genre:
                    best_match = g
                    break
            if best_match:
                gc = genre_map["coords"][best_match]
                vec.extend([gc[0] / 100.0 * W_GENRE, gc[1] / 100.0 * W_GENRE])
            else:
                # Hash genre name to a position
                h = hash(genre) % 10000
                vec.extend([(h % 100 - 50) / 50.0 * W_GENRE,
                           ((h // 100) % 100 - 50) / 50.0 * W_GENRE])
        else:
            h = hash(genre) % 10000
            vec.extend([(h % 100 - 50) / 50.0 * W_GENRE,
                       ((h // 100) % 100 - 50) / 50.0 * W_GENRE])

        # 2. Mood (2D circle)
        labels = t.get("labels", {})
        mood_vec = mood_to_vec(labels.get("mood", ""))
        vec.extend([m * W_MOOD for m in mood_vec])

        # 3. Energy (1D)
        energy = energy_map.get(labels.get("energy", ""), 0.5)
        vec.append(energy * W_ENERGY)

        # 4. Region (one-hot, compressed)
        # Use PCA-like compression: just a few dims via region index on circle
        ridx = region_to_idx.get(t["_region"], 0)
        angle = 2 * math.pi * ridx / max(n_regions, 1)
        vec.extend([math.cos(angle) * W_REGION, math.sin(angle) * W_REGION])

        # 5. Year (1D, normalized)
        year = t.get("year", "")
        if year and len(str(year)) == 4:
            y_norm = (int(year) - 1950) / 80.0  # 0..1 roughly
            y_norm = max(0, min(1, y_norm))
        else:
            y_norm = 0.5  # unknown → middle
        vec.append(y_norm * W_YEAR)

        # 6. Texture (sparse keywords)
        tex_vec = texture_to_vec(labels.get("texture", ""))
        vec.extend([v * W_TEXTURE for v in tex_vec])

        feature_vecs.append(vec)
        track_ids.append(t["id"])

    X = np.array(feature_vecs, dtype=np.float64)
    print(f"  Feature matrix: {X.shape}")

    # ── t-SNE projection ──
    n = X.shape[0]
    perp = min(40, n // 4)
    n_iter = 500

    # Normalize features
    std = X.std(axis=0)
    std[std == 0] = 1
    X = (X - X.mean(axis=0)) / std

    # Pairwise distances
    print(f"  Computing pairwise distances ({n} tracks)...")
    sum_X = np.sum(X ** 2, axis=1)
    D = sum_X[:, np.newaxis] + sum_X[np.newaxis, :] - 2 * X @ X.T
    np.fill_diagonal(D, 0)
    D = np.maximum(D, 0)

    # Compute P
    print("  Computing probability matrix...")
    target_entropy = np.log(perp)
    P = np.zeros((n, n))

    for i in range(n):
        lo, hi = 1e-10, 1e4
        Di = D[i].copy()
        Di[i] = np.inf

        for _ in range(50):
            sigma = (lo + hi) / 2
            prow = np.exp(-Di / (2 * sigma * sigma))
            prow[i] = 0
            psum = prow.sum() + 1e-10
            prow /= psum
            H = -np.sum(prow * np.log(prow + 1e-10))
            if H > target_entropy:
                hi = sigma
            else:
                lo = sigma

        prow = np.exp(-Di / (2 * sigma * sigma))
        prow[i] = 0
        P[i] = prow / (prow.sum() + 1e-10)

        if (i + 1) % 500 == 0:
            print(f"    perplexity calibrated: {i+1}/{n}")

    P = (P + P.T) / (2 * n)
    P = np.maximum(P, 1e-12)
    P *= 4.0  # early exaggeration

    # Initialize Y
    rng = np.random.RandomState(42)
    Y = rng.randn(n, 2) * 0.01
    velocity = np.zeros_like(Y)
    gains = np.ones_like(Y)
    lr = 200

    print(f"  Running t-SNE ({n_iter} iterations on {n} tracks)...")
    for it in range(n_iter):
        if it == 100:
            P /= 4.0

        diff = Y[:, np.newaxis, :] - Y[np.newaxis, :, :]
        sq_dist = np.sum(diff ** 2, axis=2)
        Q_num = 1.0 / (1.0 + sq_dist)
        np.fill_diagonal(Q_num, 0)
        Q_sum = Q_num.sum()
        Q = Q_num / (Q_sum + 1e-10)
        Q = np.maximum(Q, 1e-12)

        PQ_diff = P - Q
        mult = PQ_diff * Q_num
        grad = 4.0 * (mult.sum(axis=1, keepdims=True) * Y - mult @ Y)

        momentum = 0.8 if it > 250 else 0.5
        gains = np.where(np.sign(grad) != np.sign(velocity), gains + 0.2, gains * 0.8)
        gains = np.maximum(gains, 0.1)
        velocity = momentum * velocity - lr * gains * grad
        Y += velocity
        Y -= Y.mean(axis=0)

        if (it + 1) % 100 == 0:
            cost = np.sum(P * np.log(P / Q + 1e-10))
            print(f"    iteration {it+1}/{n_iter}, KL={cost:.4f}")

    # Normalize to [-100, 100]
    Y_min = Y.min(axis=0)
    Y_max = Y.max(axis=0)
    Y_range = Y_max - Y_min
    Y_range[Y_range == 0] = 1
    Y_norm = (Y - Y_min) / Y_range * 200 - 100

    # Build output
    track_map = {}
    for i, tid in enumerate(track_ids):
        track_map[tid] = [round(float(Y_norm[i, 0]), 2), round(float(Y_norm[i, 1]), 2)]

    out_path = os.path.join(DIR, "track_map.json")
    with open(out_path, "w") as f:
        json.dump(track_map, f)

    print(f"\n  Saved track_map.json ({len(track_map)} tracks)")
    print(f"  File size: {os.path.getsize(out_path) / 1024:.0f}KB")

    # Show some stats
    Y_arr = np.array(list(track_map.values()))
    print(f"  X range: [{Y_arr[:,0].min():.1f}, {Y_arr[:,0].max():.1f}]")
    print(f"  Y range: [{Y_arr[:,1].min():.1f}, {Y_arr[:,1].max():.1f}]")


if __name__ == "__main__":
    main()
