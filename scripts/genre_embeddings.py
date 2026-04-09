#!/usr/bin/env python3
"""
DIG — Genre embedding similarity matrix.

Computes embeddings for all genres (seed + discovered), builds a 2D projection
using t-SNE, and outputs genre_map.json for the visualization:
  - coords: {genre: {x, y}} — 2D position for each genre
  - similarity: {genre: [top 10 nearest genres]} — nearest neighbors
  - clusters: {genre: cluster_id} — broad grouping

Uses OpenAI text-embedding-3-small (cheap, fast, good for short text).
Run once, then re-run when genre pool grows significantly.
"""

import json
import os
import sys
import math

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR = ROOT
ENV_PATH = os.path.join(ROOT, ".env")

if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ── Collect all genres ──

def load_all_genres():
    """Load seed genres from discover.py's GENRE_POOL + discovered_genres.json."""
    # We import discover.py's GENRE_POOL by reading the file
    # (avoids importing spotipy and needing Spotify credentials)
    genres = set()

    # 1. Parse GENRE_POOL from discover.py
    discover_path = os.path.join(ROOT, "pipeline", "discover.py")
    if os.path.exists(discover_path):
        with open(discover_path) as f:
            content = f.read()
        # Extract all quoted strings from GENRE_POOL section
        import re
        in_pool = False
        for line in content.split("\n"):
            if "GENRE_POOL" in line and "=" in line and "{" in line:
                in_pool = True
            if in_pool:
                for m in re.findall(r'"([^"]+)"', line):
                    if len(m) >= 3 and not m.startswith("{") and m not in ("traditional", "electronic", "rock", "jazz_soul", "hip_hop", "pop_experimental", "reggae_caribbean", "classical", "country_americana", "latin", "ambient_meditative", "discovered"):
                        genres.add(m.lower())
            if in_pool and line.strip() == "}":
                in_pool = False

    # 2. Load discovered_genres.json
    discovered_path = os.path.join(DIR, "discovered_genres.json")
    if os.path.exists(discovered_path):
        with open(discovered_path) as f:
            discovered = json.load(f)
        for g in discovered:
            if isinstance(g, str) and len(g) >= 3:
                genres.add(g.lower())

    return sorted(genres)


# ── Embedding via OpenAI ──

def embed_openai(texts, batch_size=200):
    """Embed texts using OpenAI text-embedding-3-small. Returns list of vectors."""
    try:
        from openai import OpenAI
    except ImportError:
        print("  pip install openai required for embeddings")
        sys.exit(1)

    client = OpenAI(api_key=OPENAI_API_KEY)
    all_embeddings = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=batch,
        )
        for item in response.data:
            all_embeddings.append(item.embedding)
        batch_num = i // batch_size + 1
        total_batches = (len(texts) + batch_size - 1) // batch_size
        print(f"  Embedded batch {batch_num}/{total_batches}")

    return all_embeddings


# ── Embedding via Anthropic (fallback) ──

def embed_anthropic(texts):
    """Fallback: Use Claude to generate approximate 2D coordinates directly."""
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Ask Claude to place genres on a 2D map in batches
    coords = {}
    batch_size = 80

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        prompt = f"""Place each music genre on a 2D map where:
- X axis: acoustic/organic (-1) to electronic/digital (+1)
- Y axis: calm/introspective (-1) to energetic/intense (+1)

Return ONLY valid JSON: {{"genre_name": [x, y], ...}}
Use values between -1 and 1.

Genres:
{chr(10).join(batch)}"""

        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text
            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                batch_coords = json.loads(text[start:end])
                for genre, xy in batch_coords.items():
                    if isinstance(xy, list) and len(xy) == 2:
                        coords[genre.lower()] = xy
        except Exception as e:
            print(f"  (batch failed: {e})")

        import time
        time.sleep(0.3)
        batch_num = i // batch_size + 1
        total_batches = (len(texts) + batch_size - 1) // batch_size
        print(f"  Claude batch {batch_num}/{total_batches}: {len(coords)} genres mapped")

    return coords


# ── t-SNE implementation using numpy (fast) ──

import numpy as np


def cosine_similarity(a, b):
    a, b = np.array(a), np.array(b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10))


def tsne_2d(embeddings, perplexity=30, n_iter=500, lr=200):
    """t-SNE using numpy for speed. Handles ~1000 points in seconds."""
    X = np.array(embeddings, dtype=np.float64)
    n = X.shape[0]
    if n == 0:
        return []

    # Normalize for cosine → use euclidean on unit vectors
    norms = np.linalg.norm(X, axis=1, keepdims=True)
    norms[norms == 0] = 1
    X = X / norms

    # Pairwise squared Euclidean distances (on unit sphere ≈ 2*(1-cosine))
    print(f"  Computing {n}x{n} distance matrix...")
    sum_X = np.sum(X ** 2, axis=1)
    D = sum_X[:, np.newaxis] + sum_X[np.newaxis, :] - 2 * X @ X.T
    np.fill_diagonal(D, 0)
    D = np.maximum(D, 0)

    # Compute P (conditional probabilities via binary search for sigma)
    print("  Computing probability matrix...")
    perp = min(perplexity, n - 1)
    target_entropy = np.log(perp)
    P = np.zeros((n, n))

    for i in range(n):
        lo, hi = 1e-10, 1e4
        Di = D[i].copy()
        Di[i] = np.inf  # exclude self

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

    # Symmetrize
    P = (P + P.T) / (2 * n)
    P = np.maximum(P, 1e-12)

    # Early exaggeration
    P *= 4.0

    # Initialize Y
    rng = np.random.RandomState(42)
    Y = rng.randn(n, 2) * 0.01
    velocity = np.zeros_like(Y)
    gains = np.ones_like(Y)

    print(f"  Running t-SNE ({n_iter} iterations)...")
    for it in range(n_iter):
        # Remove exaggeration after 100 iterations
        if it == 100:
            P /= 4.0

        # Compute Q
        diff = Y[:, np.newaxis, :] - Y[np.newaxis, :, :]
        sq_dist = np.sum(diff ** 2, axis=2)
        Q_num = 1.0 / (1.0 + sq_dist)
        np.fill_diagonal(Q_num, 0)
        Q_sum = Q_num.sum()
        Q = Q_num / (Q_sum + 1e-10)
        Q = np.maximum(Q, 1e-12)

        # Gradients
        PQ_diff = P - Q
        mult = PQ_diff * Q_num
        grad = 4.0 * (mult.sum(axis=1, keepdims=True) * Y - mult @ Y)

        # Adaptive gains
        momentum = 0.8 if it > 250 else 0.5
        gains = np.where(np.sign(grad) != np.sign(velocity), gains + 0.2, gains * 0.8)
        gains = np.maximum(gains, 0.1)
        velocity = momentum * velocity - lr * gains * grad
        Y += velocity

        # Center
        Y -= Y.mean(axis=0)

        if (it + 1) % 100 == 0:
            cost = np.sum(P * np.log(P / Q + 1e-10))
            print(f"    iteration {it + 1}/{n_iter}, KL={cost:.4f}")

    return Y.tolist()


# ── K-means clustering using numpy ──

def kmeans(points_2d, k=15, n_iter=50):
    """K-means on 2D points using numpy."""
    pts = np.array(points_2d)
    n = pts.shape[0]
    if n <= k:
        return list(range(n))

    rng = np.random.RandomState(42)
    idx = rng.choice(n, k, replace=False)
    centroids = pts[idx].copy()
    labels = np.zeros(n, dtype=int)

    for _ in range(n_iter):
        # Assign
        dists = np.sum((pts[:, np.newaxis, :] - centroids[np.newaxis, :, :]) ** 2, axis=2)
        labels = np.argmin(dists, axis=1)
        # Update
        for c in range(k):
            mask = labels == c
            if mask.any():
                centroids[c] = pts[mask].mean(axis=0)

    return labels.tolist()


# ── Main ──

def main():
    print("\n🗺️  DIG — GENRE EMBEDDING MAP\n")

    genres = load_all_genres()
    print(f"  Total genres: {len(genres)}")

    if not genres:
        print("  No genres found. Run bootstrap_genres.py first.")
        sys.exit(1)

    # Check for existing map
    map_path = os.path.join(DIR, "genre_map.json")

    use_openai = bool(OPENAI_API_KEY)
    use_anthropic = bool(ANTHROPIC_API_KEY) and not use_openai

    if use_openai:
        print("  Using OpenAI embeddings + t-SNE projection...")

        # Embed all genres with context prefix for better quality
        texts = [f"music genre: {g}" for g in genres]
        embeddings = embed_openai(texts)

        # Compute nearest neighbors from full embeddings (vectorized)
        print("  Computing nearest neighbors...")
        E = np.array(embeddings)
        norms = np.linalg.norm(E, axis=1, keepdims=True)
        norms[norms == 0] = 1
        E_norm = E / norms
        sim_matrix = E_norm @ E_norm.T
        np.fill_diagonal(sim_matrix, -1)  # exclude self
        neighbors = {}
        for i, g in enumerate(genres):
            top_idx = np.argsort(-sim_matrix[i])[:10]
            neighbors[g] = [genres[j] for j in top_idx]

        # t-SNE projection to 2D
        # For 900+ genres, use lower perplexity and fewer iterations to keep it reasonable
        n = len(genres)
        perp = min(30, n // 4)
        n_iter = 300 if n > 500 else 500
        coords_2d = tsne_2d(embeddings, perplexity=perp, n_iter=n_iter)

        # Normalize to [-100, 100] range
        if coords_2d:
            xs = [c[0] for c in coords_2d]
            ys = [c[1] for c in coords_2d]
            x_range = max(xs) - min(xs) or 1
            y_range = max(ys) - min(ys) or 1
            scale = 200 / max(x_range, y_range)
            coords = {}
            for i, g in enumerate(genres):
                coords[g] = [
                    round((coords_2d[i][0] - (max(xs) + min(xs)) / 2) * scale, 2),
                    round((coords_2d[i][1] - (max(ys) + min(ys)) / 2) * scale, 2),
                ]

        # Cluster
        labels = kmeans(coords_2d, k=min(20, n // 10))
        clusters = {genres[i]: labels[i] for i in range(len(genres))}

    elif use_anthropic:
        print("  Using Claude for direct 2D genre mapping (no OpenAI key)...")
        raw_coords = embed_anthropic(genres)

        # Normalize to [-100, 100]
        coords = {}
        neighbors = {}
        for g in genres:
            xy = raw_coords.get(g, [0, 0])
            coords[g] = [round(xy[0] * 100, 2), round(xy[1] * 100, 2)]

        # Compute neighbors from 2D distance
        for g in genres:
            dists = []
            gx, gy = coords.get(g, [0, 0])
            for g2 in genres:
                if g2 != g:
                    g2x, g2y = coords.get(g2, [0, 0])
                    d = math.sqrt((gx - g2x) ** 2 + (gy - g2y) ** 2)
                    dists.append((g2, d))
            dists.sort(key=lambda x: x[1])
            neighbors[g] = [d[0] for d in dists[:10]]

        # Cluster from 2D coords
        points = [coords.get(g, [0, 0]) for g in genres]
        labels = kmeans(points, k=min(20, len(genres) // 10))
        clusters = {genres[i]: labels[i] for i in range(len(genres))}

    else:
        print("  No OPENAI_API_KEY or ANTHROPIC_API_KEY — cannot compute embeddings.")
        sys.exit(1)

    # Save
    genre_map = {
        "coords": coords,
        "neighbors": neighbors,
        "clusters": clusters,
        "genre_count": len(genres),
    }

    with open(map_path, "w") as f:
        json.dump(genre_map, f)

    print(f"\n  Saved genre_map.json ({len(coords)} genres mapped)")
    print(f"  Clusters: {len(set(clusters.values()))}")

    # Show some interesting neighbors
    samples = ["techno", "bossa nova", "black metal", "afrobeat", "k-pop"]
    for s in samples:
        if s in neighbors:
            print(f"  {s} → {', '.join(neighbors[s][:5])}")


if __name__ == "__main__":
    main()
