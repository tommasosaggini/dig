"""
DIG → Instagram: caption generation.

A caption is the song, the artist, and the artist's @handle when we know it.
Nothing else — no written line, no hashtags. Deterministic and offline, so the
same track always produces the same caption and the admin edits from there.

Two things this module used to add are retired but not forgotten: it still
knows how to RECOGNISE them, because captions carrying them are already in the
queue and drop_bio_line/drop_generated_extras take them back off.
"""
import re
import unicodedata

# Retired. This used to close every generated caption, pointing at the profile
# link — but it never actually made it onto a post: both published captions,
# and most of the queue, were rewritten by hand into something shorter, and the
# line was the first thing cut every time. It stays here as a pattern rather
# than a template so drop_bio_line() can take it back off the captions that
# still carry it.
BIO_LINE = "the tool i find these with is in my bio ↑"


def _article(word):
    return "an" if word[:1].lower() in "aeiou" else "a"


# Local rather than mb_resolve._norm: this module is the one part of the
# pipeline that works with no database and no network, and the caption tests
# rely on that. Pulling in a module that imports requests and lib.db to borrow
# four lines of casefolding would be a bad trade.
def _key(s):
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", s)).strip().lower()


def _tokens(s):
    return {t for t in _key(s).split() if t}


# A line that is nothing but an @mention — the handle line this module writes,
# recognised on the way back in so rewriting a caption does not stack them up.
_MENTION_LINE = re.compile(r"^@[A-Za-z0-9._]{1,30}$")


def headline(track_name, artist, ig_handle=None):
    """How every caption opens: the song, who made it, and a link to them.

    Two lines when there is a handle. On Instagram an @mention is a link
    wherever it sits, so the parenthetical "(@oscar.lang)" this used to append
    worked equally well — it just made the title line long enough to wrap on a
    phone, which is where captions are read.

        fall into u — Oscar Lang
        @oscar.lang
    """
    line = f"{track_name} — {artist}"
    handle = (ig_handle or "").strip().lstrip("@")
    return f"{line}\n@{handle}" if handle else line


def ensure_headline(caption, track_name, artist, ig_handle=None):
    """Guarantee a caption opens with the headline, keeping everything else.

    The headline is now the whole generated caption, but this still has work to
    do: it runs over captions the admin has edited by hand, so it replaces the
    opening line and leaves the rest alone rather than regenerating. That is
    also what keeps a handle appearing the moment the artist is resolved,
    without touching anything Tommaso wrote underneath.

    A first line is treated as the old headline — and so replaced — when it
    carries the artist or the track. Anything else is somebody's writing and
    gets the headline put above it.
    """
    want = headline(track_name, artist, ig_handle)
    body = (caption or "").strip()
    if not body:
        return want
    lines = body.split("\n")
    first = _tokens(lines[0])
    is_old_headline = bool(first) and (
        _tokens(artist) <= first or (_tokens(track_name) and _tokens(track_name) <= first))
    if is_old_headline:
        rest = lines[1:]
        # The handle line belongs to the headline, so it is replaced with it
        # rather than left behind. Without this, every pass over a caption
        # would push another @mention down the post.
        if rest and _MENTION_LINE.match(rest[0].strip()):
            rest = rest[1:]
    else:
        rest = [""] + lines
    return "\n".join([want] + rest).strip()


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
    return "a gem worth 45 seconds."


def _without(caption, should_drop):
    """Drop every line `should_drop` accepts, and the blank it sat between.

    Retiring a caption element always means the same two jobs — take the line
    out, and do not leave the gap where it stood — so both retirements share
    this rather than each collapsing blanks its own way.
    """
    body = (caption or "").strip()
    out, prev_blank = [], True     # leading blank suppressed: nothing above yet
    for line in body.split("\n"):
        if should_drop(line.strip()):
            continue
        blank = not line.strip()
        if blank and prev_blank:
            continue
        out.append(line)
        prev_blank = blank
    return "\n".join(out).strip()


def drop_bio_line(caption):
    """Take the retired 'tool in bio' line back out of a caption."""
    return _without(caption, lambda ln: ln == BIO_LINE)


def _is_hashtag_line(line):
    return bool(line) and all(tok.startswith("#") for tok in line.split())


# The grammar vibe_line() writes in, matched by shape rather than by content.
#
# Exact comparison against vibe_line(labels) is not enough on its own: the
# relabel-from-audio stage rewrites a track's labels after the caption was
# generated, so the stored sentence stops matching what we would write today
# and survives the sweep — which is exactly what happened to "warm, for a
# garden party." and three others on the first pass.
#
# These shapes are safe against Tommaso's own writing because the generator
# lowercases its labels and builds a phrase with no verb, while his lines read
# as sentences: "Floating about.", "Marching.", "Very fresh." all start with a
# capital and none of them is "<mood>, for a <place>."
_VIBE_SHAPES = (
    re.compile(r"^[a-z][\w '’-]*, for an? [^.]+\.$"),   # "warm, for a garden party."
    re.compile(r"^for an? [^.]+\.$"),                    # "for a rooftop sunset."
    re.compile(r"^a gem worth 45 seconds\.$"),           # the labelless fallback
)


def drop_generated_extras(caption, labels=None):
    """Take the retired vibe line and hashtag block back out of a caption.

    The same job as drop_bio_line, one generation on: a caption is now the
    song, the artist and the artist's @handle, full stop.

    Only the sentence WE wrote comes out. The rewriter runs over captions
    Tommaso has edited by hand, and deleting his writing to enforce a format
    would be a worse bug than the format — so a line has to match either the
    generator's grammar or, for the bare "<mood>." case, exactly what this
    track's labels would produce.
    """
    def generated(ln):
        if not ln:
            return False
        return (ln == vibe_line(labels)
                or any(p.match(ln) for p in _VIBE_SHAPES))

    return _without(caption, lambda ln: _is_hashtag_line(ln) or generated(ln))


def template_caption(track_name, artist, genres=None, labels=None, ig_handle=None):
    """Deterministic, offline caption: the song, the artist, their @handle.

    Nothing else. The vibe line and the hashtag set are both retired — see
    drop_generated_extras, which takes them off the captions that still carry
    them. `genres` and `labels` are kept in the signature because callers pass
    what the track knows and a future element may want it again; they are
    deliberately unused today.
    """
    return headline(track_name, artist, ig_handle)
