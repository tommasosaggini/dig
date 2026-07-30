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
BASE_HASHTAGS = ["#dig", "#musicdiscovery", "#newmusic", "#nowplaying"]

# Bio funnel line (the tool lives in the profile link, not a per-post URL).
BIO_LINE = "the tool i find these with is in my bio ↑"


def template_caption(track_name, artist, genres=None):
    """Deterministic, offline caption. Pure function — unit-testable."""
    genres = genres or []
    tags = list(BASE_HASHTAGS)
    for g in genres[:2]:
        slug = "#" + "".join(ch for ch in g.lower() if ch.isalnum())
        if slug not in tags and len(slug) > 1:
            tags.append(slug)
    lines = [
        f"{track_name} — {artist}",
        "",
        "a gem worth 30 seconds.",
        "",
        BIO_LINE,
        "",
        " ".join(tags),
    ]
    return "\n".join(lines)


def llm_caption(track_name, artist, genres=None):
    """Optional polish. Falls back to the template on any error / missing key."""
    base = template_caption(track_name, artist, genres)
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
            f"Genres: {', '.join(genres or []) or 'unknown'}\n\n"
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
