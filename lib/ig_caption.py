"""
DIG → Instagram: caption generation.

Template-first (always works, no network), with an OPTIONAL one-shot LLM polish
when a key is present. The admin always edits the result in the dashboard, so
this only has to produce a sane, on-voice starting point — never the final word.

House style: lowercase, unfussy, the track front-and-centre, a light personal
line, "tool in bio", a small fixed hashtag set. No marketing voice.
"""
import os

# A small, stable hashtag set. Kept short on purpose — walls of tags read as spam.
# `#newmusic` is deliberately NOT here: most of what DIG surfaces is old, and
# claiming otherwise on a 1969 soul record is just wrong.
BASE_HASHTAGS = ["#dig", "#musicdiscovery", "#nowplaying"]

# Genres whose punctuation survives badly as a hashtag. Stripping non-alphanum
# turns "r&b" into "rb" and "k-r&b" into "krb" — neither is a tag anyone follows.
GENRE_TAG_ALIASES = {
    "r&b": "rnb", "k-r&b": "krnb", "rock & roll": "rocknroll",
    "drum and bass": "dnb", "drum & bass": "dnb", "hip hop": "hiphop",
    "lo-fi": "lofi", "trip hop": "triphop", "synth-pop": "synthpop",
}

# Bio funnel line (the tool lives in the profile link, not a per-post URL).
BIO_LINE = "the tool i find these with is in my bio ↑"


def genre_tag(genre):
    """A hashtag for a genre, or None if it can't survive the trip."""
    g = (genre or "").strip().lower()
    if not g:
        return None
    slug = GENRE_TAG_ALIASES.get(g)
    if slug is None:
        slug = "".join(ch for ch in g if ch.isalnum())
    # Two characters is a mangling artefact, not a genre anyone searches.
    return "#" + slug if len(slug) >= 3 else None


def _article(word):
    return "an" if word[:1].lower() in "aeiou" else "a"


def vibe_line(labels):
    """One line about *this* track, from the labels the engine already assigned.

    The alternative — the same sentence under every post — is what makes an
    account read as automated. `feel` values are all places ("candlelit room",
    "empty cathedral"), so they carry a sentence naturally.
    """
    labels = labels or {}
    mood = (labels.get("mood") or "").strip().lower()
    feel = (labels.get("feel") or "").strip().lower()
    if mood and feel:
        return f"{mood}, for {_article(feel)} {feel}."
    if feel:
        return f"for {_article(feel)} {feel}."
    if mood:
        return f"{mood}."
    return "a gem worth 30 seconds."


def template_caption(track_name, artist, genres=None, labels=None):
    """Deterministic, offline caption. Pure function — unit-testable."""
    genres = genres or []
    tags = list(BASE_HASHTAGS)
    for g in genres[:2]:
        slug = genre_tag(g)
        if slug and slug not in tags:
            tags.append(slug)
    lines = [
        f"{track_name} — {artist}",
        "",
        vibe_line(labels),
        "",
        BIO_LINE,
        "",
        " ".join(tags),
    ]
    return "\n".join(lines)


def llm_caption(track_name, artist, genres=None, labels=None):
    """Optional polish. Falls back to the template on any error / missing key."""
    base = template_caption(track_name, artist, genres, labels)
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return base
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=key)
        prompt = (
            "Write a short Instagram caption for a 30-second snippet of a song i love. "
            "Voice: lowercase, plain, unpretentious, a little personal — like a friend "
            "sharing a gem, not marketing. 2-3 short lines max, then end with exactly this "
            f"line: \"{BIO_LINE}\". Then a newline and these hashtags only: "
            f"{' '.join(BASE_HASHTAGS)}.\n\n"
            f"Song: {track_name}\nArtist: {artist}\n"
            f"Genres: {', '.join(genres or []) or 'unknown'}\n"
            f"How it feels: {', '.join(v for v in ((labels or {}).get('mood'), (labels or {}).get('energy'), (labels or {}).get('texture'), (labels or {}).get('feel')) if v) or 'unknown'}\n\n"
            "Write about THIS track specifically — the same sentence under every "
            "post is what makes an account read as a bot.\n"
            "Return ONLY the caption text."
        )
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text").strip()
        return text or base
    except Exception:
        return base
