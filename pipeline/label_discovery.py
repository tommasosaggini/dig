#!/usr/bin/env python3
"""
DIG — AI labeling layer for discovery tracks.

Uses Claude Haiku to generate semantic labels (mood, energy, texture, feel,
use_case) for every track in discovery.json based on artist + track metadata.

Spotify's audio features API is deprecated (403), so we rely entirely on
Claude's music knowledge — which actually produces richer, more useful labels.

Labels are stored directly on each track in discovery.json.
Run after discover.py in the cron pipeline.
"""

import json
import os
import re
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from lib.discovery_lock import load_discovery, locked_update
from lib.artist_db import register_tracks

DIR = ROOT
ENV_PATH = os.path.join(ROOT, ".env")

# Load .env
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

if not ANTHROPIC_API_KEY:
    print("No ANTHROPIC_API_KEY set in .env — cannot label tracks.")
    sys.exit(1)

import anthropic

import httpx as _httpx
client = anthropic.Anthropic(
    api_key=ANTHROPIC_API_KEY,
    max_retries=0,
    timeout=_httpx.Timeout(60.0),   # label batches may take up to 60s
)

# ── Label a batch of tracks via Claude Haiku ──

def label_batch(tracks_batch):
    """Use Claude Haiku to generate semantic labels for a batch of tracks.
    Returns dict of track_id → labels dict.
    """
    # Build compact track list
    lines = []
    ids = []
    for t in tracks_batch:
        artist = t.get("artist", "Unknown")
        name = t.get("name", "Unknown")
        album = t.get("album", "")
        query = t.get("query", "")
        region = t.get("region", "")
        line = f"{t['id']} | {artist} — {name}"
        if album:
            line += f" [{album}]"
        if query:
            line += f" (found via: {query})"
        lines.append(line)
        ids.append(t["id"])

    prompt = f"""You are a music metadata expert. Label each track below with these fields.
Use your knowledge of the artist/song to pick the BEST match from each list.

Fields (all required — pick ONLY from the provided options):

- energy: pick exactly one of: "very low", "low", "moderate", "high", "very high"

- mood: pick exactly ONE of: "serene", "melancholic", "euphoric", "dark", "warm", "mysterious", "rebellious", "playful", "spiritual", "bittersweet", "aggressive", "dreamy", "joyful", "haunting", "tender", "chaotic", "nostalgic", "triumphant", "anxious", "peaceful"

- texture: pick 1-2 from this list (comma separated if 2): "warm analog", "crisp digital", "hazy lo-fi", "lush orchestral", "raw distorted", "shimmering synths", "deep bass", "acoustic wood", "metallic industrial", "ethereal pads", "punchy drums", "airy vocals", "gritty fuzz", "clean electric", "dense layered", "sparse minimal", "bright brass", "dark strings", "percussive tribal", "glitchy electronic"

- feel: pick exactly ONE of: "midnight drive", "sunday morning", "rainy afternoon", "desert highway", "crowded dancefloor", "empty cathedral", "forest walk", "rooftop sunset", "basement show", "ocean waves", "city night", "mountain peak", "candlelit room", "festival main stage", "train journey", "garden party", "winter cabin", "summer beach", "foggy street", "stargazing"

- use_case: pick exactly ONE of: "deep focus", "party peak", "cooking dinner", "late night alone", "road trip", "morning coffee", "workout", "meditation", "reading", "falling asleep", "house cleaning", "dinner party", "studying", "commute", "creative work", "pre-game", "yoga", "shower", "background chill", "emotional processing"

IMPORTANT: Do NOT invent new values. Use ONLY the exact strings listed above.

Return ONLY valid JSON — no markdown, no explanation. Format:
{{
  "track_id_1": {{"energy": "...", "mood": "...", "texture": "...", "feel": "...", "use_case": "..."}},
  "track_id_2": {{"energy": "...", "mood": "...", "texture": "...", "feel": "...", "use_case": "..."}}
}}

Tracks:
{chr(10).join(lines)}"""

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text
        # Extract JSON
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            result = json.loads(text[start:end])
            # The outer dict might be nested (one level of track IDs)
            # Validate structure
            if result and isinstance(next(iter(result.values())), dict):
                return result
    except json.JSONDecodeError as e:
        print(f"  (JSON parse error: {e})")
    except anthropic.RateLimitError:
        print("  (Anthropic rate limited — waiting 30s)")
        time.sleep(30)
        return {}
    except Exception as e:
        print(f"  (Haiku error: {e})")

    return {}


# ── Controlled vocabularies ──

VOCAB_MOOD = [
    "serene", "melancholic", "euphoric", "dark", "warm", "mysterious",
    "rebellious", "playful", "spiritual", "bittersweet", "aggressive",
    "dreamy", "joyful", "haunting", "tender", "chaotic", "nostalgic",
    "triumphant", "anxious", "peaceful",
]

VOCAB_TEXTURE = [
    "warm analog", "crisp digital", "hazy lo-fi", "lush orchestral",
    "raw distorted", "shimmering synths", "deep bass", "acoustic wood",
    "metallic industrial", "ethereal pads", "punchy drums", "airy vocals",
    "gritty fuzz", "clean electric", "dense layered", "sparse minimal",
    "bright brass", "dark strings", "percussive tribal", "glitchy electronic",
]

VOCAB_FEEL = [
    "midnight drive", "sunday morning", "rainy afternoon", "desert highway",
    "crowded dancefloor", "empty cathedral", "forest walk", "rooftop sunset",
    "basement show", "ocean waves", "city night", "mountain peak",
    "candlelit room", "festival main stage", "train journey", "garden party",
    "winter cabin", "summer beach", "foggy street", "stargazing",
]

VOCAB_USE_CASE = [
    "deep focus", "party peak", "cooking dinner", "late night alone",
    "road trip", "morning coffee", "workout", "meditation", "reading",
    "falling asleep", "house cleaning", "dinner party", "studying",
    "commute", "creative work", "pre-game", "yoga", "shower",
    "background chill", "emotional processing",
]

# Keyword maps: free-text keywords → controlled term
_MOOD_KEYWORDS = {
    "serene": ["serene", "calm", "tranquil", "still", "placid"],
    "melancholic": ["melanchol", "sad", "sorrow", "grief", "mournful", "gloomy", "somber", "blue"],
    "euphoric": ["euphor", "ecsta", "elat", "bliss", "exhilarat", "rush"],
    "dark": ["dark", "sinister", "ominous", "menac", "brood", "grim", "bleak"],
    "warm": ["warm", "cozy", "comfort", "gentle", "sooth", "soft"],
    "mysterious": ["myster", "enigma", "cryptic", "eerie", "uncanny", "otherworld"],
    "rebellious": ["rebel", "defian", "punk", "anger", "angry", "protest", "riot"],
    "playful": ["playful", "fun", "whims", "cheeky", "quirky", "lightheart", "silly"],
    "spiritual": ["spirit", "transcend", "sacred", "divine", "meditat", "prayer", "devotion"],
    "bittersweet": ["bittersweet", "wistful", "longing", "yearning", "poignant"],
    "aggressive": ["aggress", "intense", "fierce", "brutal", "raw power", "heavy", "hard", "violent"],
    "dreamy": ["dream", "floaty", "hazy", "surreal", "ethereal", "vaporous", "hypnot"],
    "joyful": ["joy", "happy", "celebrat", "upbeat", "cheerful", "bright", "elat", "uplift"],
    "haunting": ["haunt", "spectral", "ghost", "chill", "unsettle", "creep"],
    "tender": ["tender", "delicate", "intimate", "fragile", "vulnerab", "gentle love"],
    "chaotic": ["chaot", "frenetic", "wild", "manic", "unpredict", "frenzi", "turbul"],
    "nostalgic": ["nostalg", "retro", "reminisc", "vintage", "throwback", "memor"],
    "triumphant": ["triumph", "victor", "epic", "anthemic", "glorious", "conquer", "heroic", "soaring"],
    "anxious": ["anxious", "anxiety", "tense", "nervous", "restless", "uneasy", "paranoi"],
    "peaceful": ["peace", "zen", "restful", "quiet", "contented", "serenity", "harmony", "tranquil"],
}

_TEXTURE_KEYWORDS = {
    "warm analog": ["warm", "analog", "vintage", "tube", "tape", "retro sound"],
    "crisp digital": ["crisp", "digital", "clean", "polished", "pristine", "hi-fi"],
    "hazy lo-fi": ["hazy", "lo-fi", "lofi", "dusty", "murky", "fuzzy", "washed"],
    "lush orchestral": ["lush", "orchestral", "symphon", "sweeping", "cinematic", "grand"],
    "raw distorted": ["raw", "distort", "fuzz", "overdriv", "gritty guitar", "noise"],
    "shimmering synths": ["shimmer", "synth", "sparkle", "glisten", "bright synth"],
    "deep bass": ["deep bass", "sub bass", "low end", "bass heavy", "rumbl", "808"],
    "acoustic wood": ["acoustic", "wood", "unplugged", "organic", "folk"],
    "metallic industrial": ["metallic", "industrial", "mechanic", "clang", "harsh"],
    "ethereal pads": ["ethereal", "pads", "ambient", "atmospher", "spacious"],
    "punchy drums": ["punch", "drums", "percuss", "beat", "rhythm", "groove"],
    "airy vocals": ["airy", "vocal", "breathy", "voice", "choral", "angelic"],
    "gritty fuzz": ["gritty", "fuzz", "dirty", "rough", "abrasive"],
    "clean electric": ["clean electric", "jangly", "clean guitar", "twang"],
    "dense layered": ["dense", "layer", "thick", "wall of", "complex", "multi"],
    "sparse minimal": ["sparse", "minimal", "stripped", "bare", "simple", "empty"],
    "bright brass": ["brass", "horn", "trumpet", "trombone", "sax"],
    "dark strings": ["dark string", "cello", "viola", "violin", "bowed"],
    "percussive tribal": ["tribal", "world", "ethnic", "djembe", "hand drum"],
    "glitchy electronic": ["glitch", "electronic", "digital artifact", "stutter", "chop"],
}

_FEEL_KEYWORDS = {
    "midnight drive": ["midnight", "night drive", "late drive", "highway night", "driving"],
    "sunday morning": ["sunday", "morning", "lazy morning", "weekend", "brunch"],
    "rainy afternoon": ["rain", "afternoon", "overcast", "grey", "gray", "drizzle"],
    "desert highway": ["desert", "highway", "open road", "dusty road", "southwest"],
    "crowded dancefloor": ["dancefloor", "dance floor", "club", "dancing", "rave"],
    "empty cathedral": ["cathedral", "church", "sacred space", "reverb", "echo"],
    "forest walk": ["forest", "woods", "nature walk", "hiking", "trail"],
    "rooftop sunset": ["rooftop", "sunset", "golden hour", "dusk", "twilight", "skyline"],
    "basement show": ["basement", "underground", "diy", "small venue", "garage"],
    "ocean waves": ["ocean", "sea", "waves", "beach", "coast", "shore"],
    "city night": ["city", "urban", "neon", "downtown", "metro", "street"],
    "mountain peak": ["mountain", "peak", "summit", "altitude", "height", "vast"],
    "candlelit room": ["candle", "intimate", "dim", "quiet room", "bedroom"],
    "festival main stage": ["festival", "main stage", "arena", "stadium", "crowd"],
    "train journey": ["train", "journey", "travel", "passing", "window seat", "commut"],
    "garden party": ["garden", "party", "outdoor", "backyard", "patio", "picnic"],
    "winter cabin": ["winter", "cabin", "fireplace", "snow", "cold", "fireside"],
    "summer beach": ["summer", "beach", "tropical", "sun", "poolside", "island"],
    "foggy street": ["fog", "mist", "haze", "smoky", "dim street", "noir"],
    "stargazing": ["star", "space", "cosmos", "galax", "celestial", "sky"],
}

_USE_CASE_KEYWORDS = {
    "deep focus": ["focus", "concentrate", "study", "deep work", "programming"],
    "party peak": ["party", "turn up", "hype", "lit", "rage", "banger"],
    "cooking dinner": ["cook", "kitchen", "dinner prep", "culinary"],
    "late night alone": ["late night", "alone", "solitude", "lonely", "introspect"],
    "road trip": ["road trip", "driving", "travel", "long drive", "highway"],
    "morning coffee": ["morning", "coffee", "wake up", "sunrise", "start the day"],
    "workout": ["workout", "gym", "exercise", "running", "training", "pump"],
    "meditation": ["meditat", "mindful", "breathing", "zen", "centering"],
    "reading": ["reading", "book", "library", "literary"],
    "falling asleep": ["sleep", "slumber", "lullaby", "bedtime", "drift off", "insomnia"],
    "house cleaning": ["clean", "chores", "housework", "tidy"],
    "dinner party": ["dinner party", "hosting", "entertain", "gathering", "soiree"],
    "studying": ["study", "homework", "exam", "revision", "academic"],
    "commute": ["commut", "bus", "subway", "train ride", "transit"],
    "creative work": ["creative", "writing", "painting", "art", "design", "inspiration"],
    "pre-game": ["pre-game", "pregame", "getting ready", "warm up", "going out"],
    "yoga": ["yoga", "stretch", "flow", "flexibility"],
    "shower": ["shower", "bathroom", "singing along", "belt"],
    "background chill": ["background", "chill", "ambient", "lounge", "easy listening", "relax"],
    "emotional processing": ["emotion", "processing", "cathar", "feeling", "cry", "heal", "vent"],
}


def _best_match(value, vocab, keyword_map):
    """Find the best controlled-vocabulary match for a free-text value.
    Returns the original value if it already matches, otherwise fuzzy-matches
    via keyword overlap. Returns None if no reasonable match found.
    """
    if not value:
        return None
    val_lower = value.lower().strip()

    # Exact match
    if val_lower in vocab:
        return val_lower

    # Check if value is a comma-separated pair (for texture)
    parts = [p.strip() for p in val_lower.split(",")]
    if len(parts) > 1:
        matched_parts = []
        for part in parts:
            m = _best_match(part, vocab, keyword_map)
            if m:
                matched_parts.append(m)
        if matched_parts:
            # Deduplicate while preserving order
            seen = set()
            unique = []
            for mp in matched_parts:
                if mp not in seen:
                    seen.add(mp)
                    unique.append(mp)
            return ", ".join(unique[:2])
        # Fall through to keyword matching on the full string

    # Keyword matching: score each controlled term
    best_term = None
    best_score = 0
    for term, keywords in keyword_map.items():
        score = 0
        for kw in keywords:
            if kw in val_lower:
                score += len(kw)  # longer keyword matches score higher
        if score > best_score:
            best_score = score
            best_term = term

    if best_term and best_score > 0:
        return best_term

    # No match — return the first vocab term as a safe fallback?
    # Better: return None and let caller decide
    return None


def normalize_labels(labels):
    """Normalize a track's label dict so mood/texture/feel/use_case
    use only controlled vocabulary terms. Returns a new dict.
    """
    if not labels or not isinstance(labels, dict):
        return labels

    out = dict(labels)

    mood = _best_match(out.get("mood"), VOCAB_MOOD, _MOOD_KEYWORDS)
    if mood:
        out["mood"] = mood

    texture = _best_match(out.get("texture"), VOCAB_TEXTURE, _TEXTURE_KEYWORDS)
    if texture:
        out["texture"] = texture

    feel = _best_match(out.get("feel"), VOCAB_FEEL, _FEEL_KEYWORDS)
    if feel:
        out["feel"] = feel

    use_case = _best_match(out.get("use_case"), VOCAB_USE_CASE, _USE_CASE_KEYWORDS)
    if use_case:
        out["use_case"] = use_case

    return out


# ── Genre assignment via Claude Haiku ──

def load_canonical_genres():
    """Load the full genre list (seed + discovered) for assignment."""
    genres = set()

    # Parse GENRE_POOL from discover.py
    discover_path = os.path.join(ROOT, "pipeline", "discover.py")
    if os.path.exists(discover_path):
        with open(discover_path) as f:
            content = f.read()
        in_pool = False
        for line in content.split("\n"):
            if "GENRE_POOL" in line and "=" in line and "{" in line:
                in_pool = True
            if in_pool:
                for m in re.findall(r'"([^"]+)"', line):
                    if len(m) >= 3 and m not in ("traditional", "electronic", "rock", "jazz_soul", "hip_hop", "pop_experimental", "reggae_caribbean", "classical", "country_americana", "latin", "ambient_meditative", "discovered"):
                        genres.add(m.lower())
            if in_pool and line.strip() == "}":
                in_pool = False

    # Load genres from the DB (replaces discovered_genres.json)
    try:
        from lib.genres import load as db_load_genres
        for g in db_load_genres():
            if len(g) >= 3:
                genres.add(g.lower())
    except Exception:
        pass

    return sorted(genres)


# Build a compact genre reference (top ~200 by relevance for the prompt)
# Full list is too long for a prompt — we give Claude a representative sample
# and allow it to pick from it or suggest close matches
def build_genre_reference():
    """Build a compact genre list for the Claude prompt."""
    all_genres = load_canonical_genres()
    # If manageable, use all. Otherwise sample broadly.
    if len(all_genres) <= 300:
        return all_genres
    # Take every Nth to get ~250 spread across the alphabet
    step = max(1, len(all_genres) // 250)
    return all_genres[::step]


GENRE_REF = None  # lazy loaded


def assign_genres_batch(tracks_batch):
    """Use Claude Haiku to assign 1-3 canonical genres per track."""
    global GENRE_REF
    if GENRE_REF is None:
        GENRE_REF = build_genre_reference()

    lines = []
    for t in tracks_batch:
        artist = t.get("artist", "Unknown")
        name = t.get("name", "Unknown")
        album = t.get("album", "")
        query = t.get("query", "")
        labels = t.get("labels", {})
        line = f"{t['id']} | {artist} — {name}"
        if album:
            line += f" [{album}]"
        if query:
            line += f" (search: {query})"
        if labels and labels.get("texture"):
            line += f" (texture: {labels['texture']})"
        lines.append(line)

    # Include ~200 reference genres in prompt
    genre_sample = ", ".join(GENRE_REF[:200])

    prompt = f"""You are a music genre classifier. For each track, assign 1-3 genres from this reference list (or very close variants).

REFERENCE GENRES (pick from these when possible):
{genre_sample}

Rules:
- Pick 1-3 genres that best describe each track. Use the MOST SPECIFIC genre that applies.
- If the track fits a genre not in the list, use the closest match from the list.
- Order genres from most to least relevant.
- Use lowercase.

Return ONLY valid JSON:
{{"track_id": ["genre1", "genre2"], ...}}

Tracks:
{chr(10).join(lines)}"""

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
            result = json.loads(text[start:end])
            # Validate: each value should be a list of strings
            validated = {}
            for tid, genres in result.items():
                if isinstance(genres, list) and all(isinstance(g, str) for g in genres):
                    validated[tid] = [g.lower() for g in genres[:3]]
            return validated
    except json.JSONDecodeError as e:
        print(f"  (genre JSON parse error: {e})")
    except anthropic.RateLimitError:
        print("  (Anthropic rate limited — waiting 30s)")
        time.sleep(30)
        return {}
    except Exception as e:
        print(f"  (genre assignment error: {e})")

    return {}


# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════

print("\n🏷️  DIG — AI LABELING\n")

# Load discovery pool

discovery = load_discovery()

# Pending label/genre mutations: track_id → dict of fields to set on the track.
# e.g. {"labels": {...}, "genres": [...]}
_pending_mutations = {}

def _flush_mutations():
    """Apply pending label/genre mutations to discovery.json atomically."""
    global _pending_mutations
    mutations = _pending_mutations
    _pending_mutations = {}
    if not mutations:
        return
    def _apply(disk_data):
        for region, tracks in disk_data.items():
            for t in tracks:
                mut = mutations.get(t["id"])
                if mut:
                    t.update(mut)
    locked_update(_apply)

# Collect all tracks that need labels (both Spotify and YouTube)
unlabeled = []
total = 0
for region, tracks in discovery.items():
    for i, t in enumerate(tracks):
        total += 1
        t["region"] = region  # attach region for context
        if not t.get("labels"):
            unlabeled.append((region, i, t))

already = total - len(unlabeled)
print(f"  Total tracks: {total}")
print(f"  Already labeled: {already}")
print(f"  Need labels: {len(unlabeled)}")

if not unlabeled:
    print("\n  All tracks labeled. Nothing to do.")
    sys.exit(0)

# Process in batches of 25 (good balance of context/cost/reliability)
BATCH_SIZE = 25
labeled_count = 0
failed_batches = 0
MAX_FAILURES = 5

for batch_start in range(0, len(unlabeled), BATCH_SIZE):
    if failed_batches >= MAX_FAILURES:
        print(f"\n  Too many failures ({MAX_FAILURES}), stopping.")
        break

    batch = unlabeled[batch_start:batch_start + BATCH_SIZE]
    tracks_for_haiku = [t for _, _, t in batch]

    ai_labels = label_batch(tracks_for_haiku)

    if not ai_labels:
        failed_batches += 1
        continue

    batch_labeled = 0
    for region, idx, track in batch:
        track_labels = ai_labels.get(track["id"], {})
        if track_labels and "energy" in track_labels:
            discovery[region][idx]["labels"] = track_labels
            _pending_mutations.setdefault(track["id"], {})["labels"] = track_labels
            batch_labeled += 1

    labeled_count += batch_labeled
    batch_num = batch_start // BATCH_SIZE + 1
    total_batches = (len(unlabeled) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"  Batch {batch_num}/{total_batches}: {batch_labeled}/{len(batch)} labeled (total: {labeled_count})")

    # Flush every batch — never risk losing work
    _flush_mutations()

    time.sleep(0.3)

# Final save after labeling
_flush_mutations()

final_labeled = sum(
    1 for tracks in discovery.values()
    for t in tracks
    if t.get("labels")
)
print(f"\n✓ Labeling done. {final_labeled}/{total} tracks now have labels.")

# ══════════════════════════════════════════════
# PASS 1.5: NORMALIZE LABELS TO CONTROLLED VOCABULARY
# ══════════════════════════════════════════════

print("\n🔧 NORMALIZING LABELS\n")

normalized_count = 0
for region, tracks in discovery.items():
    for i, t in enumerate(tracks):
        if t.get("labels") and isinstance(t["labels"], dict):
            original = t["labels"]
            normed = normalize_labels(original)
            if normed != original:
                discovery[region][i]["labels"] = normed
                _pending_mutations.setdefault(t["id"], {})["labels"] = normed
                normalized_count += 1

if normalized_count > 0:
    _flush_mutations()
    print(f"  Normalized {normalized_count} tracks to controlled vocabulary.")
else:
    print("  All labels already use controlled vocabulary.")

# ══════════════════════════════════════════════
# PASS 2: GENRE ASSIGNMENT
# ══════════════════════════════════════════════

print("\n🎸 GENRE ASSIGNMENT\n")

# Collect tracks without genres
need_genres = []
have_genres = 0
for region, tracks in discovery.items():
    for i, t in enumerate(tracks):
        if t.get("genres") and isinstance(t["genres"], list) and len(t["genres"]) > 0:
            have_genres += 1
        else:
            need_genres.append((region, i, t))

print(f"  Already have genres: {have_genres}")
print(f"  Need genre assignment: {len(need_genres)}")

if need_genres:
    GENRE_BATCH_SIZE = 30
    genre_assigned = 0
    genre_failures = 0

    for batch_start in range(0, len(need_genres), GENRE_BATCH_SIZE):
        if genre_failures >= MAX_FAILURES:
            print(f"\n  Too many genre failures ({MAX_FAILURES}), stopping.")
            break

        batch = need_genres[batch_start:batch_start + GENRE_BATCH_SIZE]
        tracks_for_genre = [t for _, _, t in batch]

        genre_results = assign_genres_batch(tracks_for_genre)

        if not genre_results:
            genre_failures += 1
            continue

        batch_assigned = 0
        for region, idx, track in batch:
            track_genres = genre_results.get(track["id"], [])
            if track_genres:
                discovery[region][idx]["genres"] = track_genres
                _pending_mutations.setdefault(track["id"], {})["genres"] = track_genres
                batch_assigned += 1

        genre_assigned += batch_assigned
        batch_num = batch_start // GENRE_BATCH_SIZE + 1
        total_batches = (len(need_genres) + GENRE_BATCH_SIZE - 1) // GENRE_BATCH_SIZE
        print(f"  Batch {batch_num}/{total_batches}: {batch_assigned}/{len(batch)} assigned (total: {genre_assigned})")

        # Flush every batch — never risk losing work
        _flush_mutations()

        time.sleep(0.3)

    # Final save
    _flush_mutations()

    print(f"\n✓ Genre assignment done. {genre_assigned}/{len(need_genres)} tracks now have genres.")
else:
    print("  All tracks have genres. Nothing to do.")

# ══════════════════════════════════════════════
# PASS 3: GENRE PROPAGATION (tracks ↔ artist_db)
# ══════════════════════════════════════════════

print("\n🔗 GENRE PROPAGATION\n")

# Re-read discovery from disk (flush above wrote the latest)
discovery = load_discovery()

# 3a. Push track genres → artist_db
# Every track with genres updates its artist's genre list
push_tracks = []
for region, tracks in discovery.items():
    for t in tracks:
        if t.get("genres"):
            push_tracks.append(t)

if push_tracks:
    # register_tracks updates artist entries with any new genres from tracks
    # Process in chunks to avoid holding too much in memory
    for i in range(0, len(push_tracks), 500):
        chunk = push_tracks[i:i+500]
        # Infer region from discovery structure
        register_tracks(chunk)
    print(f"  Pushed genres from {len(push_tracks)} tracks → artist_db")

# No track-to-track genre inheritance — each track keeps only the genres
# Claude assigned specifically to it. Artists accumulate all their tracks'
# genres, but that knowledge doesn't flow back to individual tracks.

# Final stats  (labels live under t["labels"] as a nested dict)
discovery = load_discovery()
total = sum(len(tracks) for tracks in discovery.values())
fully_labeled = sum(
    1 for tracks in discovery.values()
    for t in tracks
    if t.get("labels", {}).get("energy") and t.get("labels", {}).get("mood") and t.get("labels", {}).get("texture")
)
with_any_label = sum(1 for tracks in discovery.values() for t in tracks if t.get("labels", {}).get("energy"))
pct_full = (100 * fully_labeled // total) if total else 0
pct_any = (100 * with_any_label // total) if total else 0
print(f"\n📊 Final: {total} tracks | {with_any_label} with labels ({pct_any}%) | {fully_labeled} fully labeled ({pct_full}%)")
