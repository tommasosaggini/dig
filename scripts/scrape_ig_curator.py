#!/usr/bin/env python3
"""DIG — pull what a music curator has posted about, from their public IG.

WHY THIS ENDPOINT AND NOT THE GRAPH API. Meta's `business_discovery` only
reaches accounts that are Business/Creator, and it is the wrong tool anyway:
these are small independent curators, and the thing we want — every track they
have ever named — is in the captions of a few hundred reels. The web app's own
JSON endpoints answer for any PUBLIC profile with no login, which is what the
browser does when anyone opens the page. Measured 2026-08-05: dublysm 191
posts, stirred.blessings 378, 2700audit 85.

Two calls, both unauthenticated:
  * users/web_profile_info/?username=X   -> numeric id, counts, first 12 posts
  * clips/user/ (POST, target_user_id)   -> paginated reels, with captions

NOT AVAILABLE, checked rather than assumed (2026-08-05): the following list is
401 `require_login`, and `edge_related_profiles` comes back empty for every one
of these accounts. So there is no follow-graph and no similar-accounts graph to
crawl from here. What IS in the data is the @mentions and the record labels the
curators name themselves, and this prints both — a curator tagging another
curator, or repeatedly naming a label, is the expansion signal that actually
survives being unauthenticated.

The captions are often STRUCTURED, which is the real find. dublysm writes:

    Artist – Track  Label : X  Album : Y  Année : 2008  Localisation : UK
    Style : Experimental, ...

so artist, track, label, year, COUNTRY and GENRE all come out of the caption —
the last two being exactly what Dig stratifies on and what MusicBrainz is
otherwise asked for. Curators who write free-form still yield artist + track
through the fallback.

Dry-run by default: prints what it would extract and never writes. Feed the
JSON to scripts/ingest_curator.py when the yield looks right.

  python3 scripts/scrape_ig_curator.py dublysm 2700audit --out cands.json
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import http.cookiejar
import urllib.error
import urllib.parse
import urllib.request

PROFILE = "https://www.instagram.com/api/v1/users/web_profile_info/?username={u}"
CLIPS = "https://www.instagram.com/api/v1/clips/user/"
# The public web client's own app id. Without it these endpoints answer with
# the logged-out HTML shell instead of JSON.
HEADERS = {
    "x-ig-app-id": "936619743392459",
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
}
PACE_S = 2.5          # polite; IG starts answering "wait a few minutes" well before this matters

# The clips endpoint is a POST, and IG rejects a POST without the CSRF token
# that its own pages carry: `HTTP 403 CSRF token missing or incorrect`, which
# is what stops a scrape dead after the free first page. The token is just a
# cookie handed out to anyone who loads instagram.com, so one GET earns it.
_OPENER = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
_CSRF = None


def _prime():
    """Fetch the csrftoken cookie once per run."""
    global _CSRF
    if _CSRF:
        return _CSRF
    try:
        req = urllib.request.Request("https://www.instagram.com/", headers=HEADERS)
        _OPENER.open(req, timeout=30).read(2048)
    except Exception:
        pass
    for c in _OPENER.handlers[-1].cookiejar if hasattr(_OPENER.handlers[-1], "cookiejar") else []:
        pass
    for h in _OPENER.handlers:
        jar = getattr(h, "cookiejar", None)
        if jar:
            for c in jar:
                if c.name == "csrftoken":
                    _CSRF = c.value
    return _CSRF

# Field labels curators use, in the languages they use them in. Everything from
# the first one onward is metadata; everything before it is "Artist – Track".
FIELD = re.compile(
    r"\b(label|album|ann[ée]e|year|localisation|location|pays|country|style|genre|"
    r"format|cat[ae]logue|ref)\s*[:：]", re.IGNORECASE)
# Curators split artist from title with an en/em dash far more often than a
# hyphen, and hyphens appear INSIDE names ("Jean-Luc"), so the dashes are tried
# first and the hyphen only when padded with spaces.
SPLIT = re.compile(r"\s+[–—]\s+|\s+-\s+")
# Prose curators name the track in quotes and the artist after "by":
#   swimming in "Strumenti dell'Indugio" by @g.l.o.m.a.r.i — ...
# stirred.blessings writes this way for nearly every post, and the dash rule
# above reads the whole sentence as one artist name, so 378 posts yielded 1.
# Delimiters are PAIRED rather than pooled into one character class. Pooled,
# the class excluded every quote mark from the title as well, so an apostrophe
# inside a title ended it: \u201cStrumenti dell'Indugio\u201d matched from the apostrophe
# and yielded "Indugio". Each opener now only bans its own closer.
QUOTED_BY = re.compile(
    r"(?:\u201c([^\u201d\n]{2,90})\u201d"          # \u201c \u2026 \u201d
    r"|\u201e([^\u201c\u201d\n]{2,90})[\u201c\u201d]"   # \u201e \u2026 \u201c/\u201d
    r"|\u00ab([^\u00bb\n]{2,90})\u00bb"            # \u00ab \u2026 \u00bb
    r"|\"([^\"\n]{2,90})\""                        # " \u2026 "
    r"|'([^'\n]{2,90})')"                          # ' \u2026 '
    r"\s+by\s+@?([^\n\u2014\u2013(|]{2,90})",
    re.IGNORECASE)
# 2700audit writes: NNN/2700 | „Title" by ARTIST from „Album", 22 September 2003.
# The artist runs up to " from ", and without cutting there the capture was
# "POLMO POLPO from „Like Hearts Swelling", September 22, 2003." — over the
# length limit, so every one of those 85 posts was silently dropped.
ARTIST_TAIL = re.compile(r"\s+(from|out\s+on|on)\s+.*$", re.IGNORECASE | re.DOTALL)
# The same sentence WITHOUT quotation marks, behind a running counter:
#   041/2700  Hetkonen by Vladislav Delay | From the album Kuopio, ...
#   039/2700 | No Gamet by Yimino, MC Go-Lightly from Autonoe Vera, 2009
# 40 of 2700audit's 43 unparsed captions are this and nothing else. The
# counter is what makes it safe to read an unquoted "X by Y" as a citation
# rather than as prose — without that anchor the same rule would strip-mine
# every sentence containing the word "by".
NUMBERED_BY = re.compile(
    r"^[\s\u2060]*\d{1,5}\s*/\s*\d{2,5}[\s\u2060]*[|·:.\u2013\u2014-]*\s*"
    r"(.+?)\s+by\s+(.+)$", re.IGNORECASE | re.MULTILINE)
MENTION = re.compile(r"@([A-Za-z0-9_.]{2,30})")
# Set per handle by main(): a label's own catalogue line reads exactly like
# an artist headline, and the handle is what tells them apart.
_LABEL_HINT = None
# The sleeve-note grammar. lyon__beatsonandon writes every post as
#
#     Жалам хар (A Black Horse) by The Bayan Mongol Variety Group (1980) 🇲🇳✨
#     Восточный Сувенир (Oriental Souvenir) – Gunesh Ensemble (1980) 🌙✨
#
# — TRACK first, artist second, which is the reverse of every other curator
# here. Read with the dash rule it produced 81 perfectly-formed rows with the
# two fields swapped and "Lord Rhaburn (1979) 🇧🇿✨" as an artist name.
#
# The bracketed year is the anchor that makes this a citation rather than
# prose, and it is also why the two variants are kept apart: "by" STATES that
# the artist follows, while a dash states nothing — dublysm writes
# "Artist – Track" with the same shape. So the dash form is parsed but left
# ambiguous, and main() resolves it per handle from that curator's own
# explicit "by" posts. Nothing is swapped on a hunch.
#
# Two anchors, either of which marks the line as a citation rather than prose:
# a bracketed year, or a trailing run of emoji. 39 of these 108 captions carry
# no year — "Colourful Environment – Gboyega Adelaja 🇳🇬" — and reading those
# with the generic dash rule left the emoji inside the artist name.
EMOJI = ("[\U0001F000-\U0001FAFF←-⇿⌀-➿⬀-⯿"
         "️‍⁠]")
_YEAR = r"(?:\s*\((?P<year>1[89]\d{2}|20\d{2})s?\))?"
_TAIL = r"(?P<tail>(?:\s*" + EMOJI + r")+)?\s*$"
SLEEVE_BY = re.compile(
    r"^(?P<a>.+?)\s+by\s+(?P<b>.+?)" + _YEAR + _TAIL, re.IGNORECASE)
SLEEVE_DASH = re.compile(r"^(?P<a>.+?)\s*[–—]\s*(?P<b>.+?)" + _YEAR + _TAIL)
DECOR_TAIL = re.compile(r"(?:\s*" + EMOJI + r")+\s*$")
EMOJI_ANY = re.compile(EMOJI)
# A side wrapped in quotes is the track — the curator marking the title.
QUOTED_SIDE = re.compile(r"^[“\"'][^“”\"']{2,}[”\"']$")
# A flag emoji is a pair of regional-indicator letters, so it decodes straight
# to the ISO country code — the field Dig stratifies on and the one these
# captions carry for free.
FLAG = re.compile("[\U0001F1E6-\U0001F1FF]{2}")
# England/Scotland/Wales are not regional-indicator pairs but tag sequences on
# U+1F3F4, so FLAG never sees them and they would otherwise sit in the artist
# name. Matched only to be stripped; the country comes from the words beside
# them ("🏴 (Scotland, UK 🇬🇧)").
TAG_FLAG = re.compile("\U0001F3F4[\U000E0000-\U000E007F]+")
HASHTAG = re.compile(r"#(\w+)")
# Curatorial tags, not genres. Everything else in the block is a real style or
# the country, and the country is already known from the flag.
TAG_STOPLIST = {"raregrooves", "raregroove", "worldmusic", "music", "vinyl",
                "vinylcollection", "digging", "cratedigging", "groove",
                "grooves", "obscure", "rare"}


# ── the label-announcement grammar ──────────────────────────────────────────
# habibifunk is a REISSUE LABEL, not a track-a-day curator: 225 posts, 6 of
# which cite a track. The other 219 are announcements, and the ones that matter
# open with the release they are announcing —
#
#     NECHAZZ, AMMAN, JORDAN, 1988 (Habibi Funk 036): after nechazz recorded…
#     OUT TODAY => AHMED MALEK 🇩🇿 (Habibi Funk 027): …
#
# so there is no track to be had, but there IS an artist, and artists are what
# the pool is short of. These rows come out with track=None.
#
# The gate is a year or a catalogue number in the headline, which is what
# separates a release from "FREE STICKERS:" and "DJ GIGS:". It does not need to
# be airtight — resolve_curator_artists puts every name to MusicBrainz, and
# "BACK ON STAGE AFTER 40 YEARS" resolves to nothing.
RELEASE_HEAD = re.compile(r"^([^:\n]{3,80}):")
YEAR4 = re.compile(r"\b(?:19|20)\d{2}\b")
CATALOGUE = re.compile(r"\(\s*[^)]*\d{2,3}\s*\)")
# Label admin, and the label's own catalogue standing in for an artist name.
ADMIN_HEAD = re.compile(
    r"\b(sticker|gig|playlist|tour|pop.?up|link in bio|mix|update|merch|shop|"
    r"order|sale|giveaway|box set|edition|single|repress|vinyl|bandcamp|"
    r"soundcloud|print)\b", re.I)
LEAD_IN = re.compile(
    r"^(out today|happy release day|introducing|in memory of|up next|out now|"
    r"coming soon|new)\b[\s=>\-–—:]*", re.I)
NAME_CUT = re.compile(r"\s*(?:,|\(|\s-\s|\sx\s|\s=>\s|\sfeat\b).*$", re.I)


def _parse_release_headline(text, label_hint=None):
    head = " ".join((text or "").split("\n")[0].split())
    m = RELEASE_HEAD.match(head)
    if not m:
        return None
    head = m.group(1)
    # A catalogue number counts whether or not it is bracketed: this grammar
    # now runs before the dash rule, so "HABIBI FUNK 019 - FERKAT AL ARD" has
    # to be recognised here or the dash rule takes it.
    # _key() drops spaces, so "HABIBI FUNK 019" and the handle "habibifunk"
    # meet as "habibifunk019" and "habibifunk".
    catno = CATALOGUE.search(head) or (
        label_hint and re.search(re.escape(_key(label_hint)) + r"\d{2,3}",
                                 _key(head)))
    if not (YEAR4.search(head) or catno):
        return None
    stem = LEAD_IN.sub("", head)

    def _usable(cand):
        cand = DECOR_TAIL.sub("", (cand or "").strip(" -–—.·")).strip()
        if not cand or len(cand) < 3 or ADMIN_HEAD.search(cand):
            return None
        # "HABIBI FUNK 019" is the catalogue, not the act; so is a bare year.
        if re.fullmatch(r"[\W\d\s]+", cand) or re.search(r"\d{2,}\s*$", cand):
            return None
        if label_hint and _key(label_hint) and _key(label_hint) in _key(cand):
            return None
        return cand

    # The artist is usually what the headline opens with, but not when the
    # catalogue or the year goes first — "HABIBI FUNK 019 - FERKAT AL ARD",
    # "2022 - FERKAT AL ARD". Rejecting the head and stopping there lost the
    # band, so the other side of the dash gets the same test.
    name = _usable(NAME_CUT.sub("", stem))
    if not name and " - " in stem:
        name = _usable(NAME_CUT.sub("", stem.split(" - ", 1)[1]))
    if not name:
        return None
    yr = YEAR4.search(head)
    return {
        "raw": head,
        "artist": name, "track": None,
        "label": label_hint,
        "year": yr.group(0) if yr else None,
        "country": _flag_country(head),
        # The headline itself, minus the artist, as a PLACE hint: these posts
        # name the country in words ("ANDROMEDA, LEBANON 1982") far more often
        # than they fly a flag, and mb_resolve can read either.
        "place": head.replace(name, " ").strip(" ,-–—()") or None,
        "style": _hashtag_styles(text),
        "orient": "artist-only",
    }


def _key(s):
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def _flag_country(text):
    m = FLAG.search(text or "")
    if not m:
        return None
    return "".join(chr(ord(c) - 0x1F1E6 + ord("A")) for c in m.group(0))


def _hashtag_styles(text, country_word=None):
    out = []
    for t in HASHTAG.findall(text or ""):
        low = t.lower()
        if low in TAG_STOPLIST or (country_word and low == country_word.lower()):
            continue
        out.append(low)
    return ", ".join(out[:4]) or None


def _parse_sleeve(text):
    """The sleeve-note grammar, or None. See SLEEVE_BY above.

    Always fills the pair the house way — artist first — and reports what the
    caption actually claimed in `orient`: 'track-first' when it said "by" and
    therefore told us, None when it only used a dash. Every swap then happens
    in exactly one place (main), so the two variants cannot end up swapped a
    different number of times, which is precisely the bug this shape avoids.
    """
    head = " ".join((text or "").split("\n")[0].split())
    # Take the curator's list furniture and any leading country off the head
    # BEFORE matching, so neither ends up inside a name. "album recommendation:
    # The Underdark – Wormwitch" and "🇬🇧 (United Kingdom) Steve Winwood – While
    # You See A Chance" both matched cleanly and both shipped an unresolvable
    # artist. The country is carried out rather than re-read, because by the
    # time the pattern matches the flag it named is gone.
    head = ITEM_NUM.sub("", head).strip()
    lead_country, head = _strip_country_prefix(head)
    for pat, orient in ((SLEEVE_BY, "track-first"), (SLEEVE_DASH, None)):
        m = pat.match(head)
        if not m:
            continue
        # One of the two anchors must be present, or this is just prose with a
        # dash in it — "The Levantine groove has never sounded this... cinematic."
        # The emoji is looked for anywhere in the line, not only at the end:
        # "Dur-Dur Band 🇸🇴 – Yabaal" decorates the artist rather than the line.
        # `lead_country` counts as an anchor in its own right: it was a flag
        # until two lines ago, and stripping it must not quietly demote the
        # caption to unanchored prose.
        if not (m.group("year") or EMOJI_ANY.search(head) or lead_country):
            continue
        # An emoji can sit on either side ("Dur-Dur Band 🇸🇴 – Yabaal"), so strip
        # both rather than trusting the tail group to have caught it.
        first = DECOR_TAIL.sub("", m.group("a")).strip(" .·|")
        second = DECOR_TAIL.sub("", m.group("b")).strip(" .·|")
        # Quotation marks around one side name the TRACK, and that settles the
        # orientation as firmly as the word "by" does. Without this,
        # 'T.P. Orchestre Poly-Rythmo – "Aihe Ni Kpe We"' inherits the handle's
        # majority reading and files the orchestra as the song.
        if QUOTED_SIDE.match(second):
            orient = "artist-first"
        elif QUOTED_SIDE.match(first):
            orient = "track-first"
        first, second = first.strip("“”\"'"), second.strip("“”\"'")
        if not first or not second or len(first) > 120 or len(second) > 120:
            continue
        # Same sentence-vs-citation guard the other grammars use. Loosening the
        # anchor to "an emoji anywhere" widens what reaches here, and these
        # captions are prose with emoji in them more often than not.
        if not _plausible(first, second):
            continue
        country = lead_country or _flag_country(head)
        return {
            "raw": head,
            # Written order, unswapped. main() applies the orientation.
            "artist": first, "track": second,
            "label": None,
            "year": m.group("year"),
            "country": country,
            "style": _hashtag_styles(text),
            "orient": orient,
        }
    return None
# THE HIGHEST-YIELD GRAMMAR OF ALL, and the one worth looking for first.
# stirred.blessings writes an essay and then lists every track in it:
#
#     Tracks mentioned:
#     Boards of Canada - Everything You Do Is A Balloon
#     Boards of Canada - Hi Scores
#
# One post is therefore many candidates, not one. Reading these posts with the
# single-track rules returned 1 candidate from 36 posts; the list is the whole
# content and the prose above it is the joke.
LIST_HEADER = re.compile(
    r"^\s*(tracks?\s+mentioned|tracklist|track\s*list|tracks?|songs?\s+mentioned|songs?)\s*[:：]\s*$",
    re.IGNORECASE | re.MULTILINE)


# ── the flag-headed block grammar ───────────────────────────────────────────
# doubleudiego files everything by country and writes the flag on its own line
# (or in front of the pair), then one or more citations under it:
#
#     Best Songs of All Time: Part 94
#
#     🇬🇧 (United Kingdom)
#     The Smiths - Still Ill (1984)
#     The Smiths - Barbarism Begins At Home (1985)
#
#     🇯🇵 (Japan) Les Rallizes Dénudés
#     Songs:
#     1. The Last One_1980 (2025) [Recorded 1980]
#
# Read with the single-track rules this yielded 55 candidates from 274
# captions, and the 55 were damaged: 37 artist names carried the flag and the
# parenthesised country ("🇬🇧 (United Kingdom) The Smiths"), which the Spotify
# fuzzy match cannot hit, while country and year — both sitting right there in
# the string — came back None on every single row.
#
# The country header is the whole point. It is the field Dig stratifies on,
# these captions state it explicitly, and it is otherwise an mb_resolve call
# per artist to recover something we were already told.
COUNTRY_PAREN = re.compile(r"^\s*\(\s*(?P<c>[^)]{2,60}?)\s*\)")
# "Album 1:", "1.", "Song # 4 on:", "2)", and the unnumbered "Album:" — the
# curator's own list furniture. Two alternatives rather than one loose pattern:
# a keyword may drop its number ("Album: Slowdive - Souvlaki", which otherwise
# ships an artist called "Album: Slowdive"), but a bare number may not drop its
# separator, or every title opening with a digit loses its first character.
ITEM_NUM = re.compile(
    r"^\s*(?:(?:album|song|track|disc|no)"
    r"(?:\s+(?:review|recommendation|rec|pick|of\s+the\s+day))?s?"
    r"\s*#?\s*\d{0,2}\s*(?:on)?\s*[:.)\-–—]"
    r"|#?\s*\d{1,2}\s*[:.)]"
    r"|\d{1,2}\s+[-–—])\s*", re.I)
# A BARE leading number — "1 Broadcast - Black Cat (2005)" — with no separator
# after it. Stripping this unconditionally would eat the name off "50 Cent"
# and "2 Chainz", so it is only applied to a caption that demonstrably numbers
# its lines: see _is_numbered_list.
BARE_NUM = re.compile(r"^\s*\d{1,2}\s+(?=\S)")
NUMBERED_LINE = re.compile(r"^\s*\d{1,2}[\s.):\-–—]")
# "(1986)" closing the line, and "[Recorded 1980]" after it. Both are the
# release claim, not part of the title.
TRAIL_YEAR = re.compile(r"\s*\(\s*(?P<y>1[89]\d{2}|20\d{2})s?\s*\)\s*$")
TRAIL_BRACKET = re.compile(r"\s*\[(?P<b>[^\]]{0,60})\]\s*$")
_BRACKET_YEAR = re.compile(r"\b(?P<y>1[89]\d{2}|20\d{2})\b")
# A line that only announces a list. The artist named above it owns the
# numbered lines that follow.
SONGS_HEADER = re.compile(r"^\s*(songs?|tracks?|tracklist)\s*[:：]\s*$", re.I)


def _strip_country_prefix(line):
    """(country_or_None, remainder). Consumes a leading flag and, if one
    follows it, the parenthesised country words — "🇨🇳 (China, Macau 🇲🇴)"
    names China. Nothing is consumed unless a flag actually led the line, so
    a title that merely opens with a bracket is left alone."""
    rest = line.strip()
    lead = TAG_FLAG.match(rest) or FLAG.match(rest)
    if not lead:
        return None, rest
    country = _flag_country(lead.group(0))       # None for the tag flags
    rest = rest[lead.end():].strip()
    m = COUNTRY_PAREN.match(rest)
    if m:
        # "Scotland, UK 🇬🇧" — the first component is the specific place, and
        # it beats the ISO code because the code is the parent country.
        words = FLAG.sub("", m.group("c")).split(",")[0].strip()
        if words:
            country = words
        rest = rest[m.end():].strip()
    return country or None, rest


def _split_year(track):
    """Pull the release claim off the end of a title. Returns (title, year)."""
    year = None
    m = TRAIL_BRACKET.search(track)
    if m:
        by = _BRACKET_YEAR.search(m.group("b"))
        if by:
            year = by.group("y")
            track = track[:m.start()].strip()
    m = TRAIL_YEAR.search(track)
    if m:
        year = year or m.group("y")
        track = track[:m.start()].strip()
    return track.strip(" .·|-–—"), year


def _leads_with_a_flag(text):
    """Does any line OPEN with a flag? That is what "filed under a country"
    looks like, and it is the only shape this grammar may claim."""
    for raw in (text or "").split("\n"):
        line = raw.strip()
        if line and (TAG_FLAG.match(line) or FLAG.match(line)):
            return True
    return False


def _is_numbered_list(text):
    """Does this caption number its lines? Two is a list; one is a band name.

    The whole point is to keep "50 Cent - In Da Club" intact while stripping
    the 1 off "1 Broadcast - Black Cat". Nothing about a single line tells
    those apart — what does is whether the lines AROUND it are numbered too.
    """
    return sum(1 for ln in (text or "").split("\n")
               if NUMBERED_LINE.match(ln.strip())) >= 2


def _parse_flagged_block(text):
    """A country-filed caption -> LIST of rows, or [] if it is not one.

    Walks the caption line by line holding two pieces of state: the country
    most recently declared by a flag, and — only after a `Songs:` header —
    the artist a bare numbered line belongs to. Both are scoped to the
    caption, so a post that changes country mid-list (an album rundown) files
    each entry under the flag directly above it.
    """
    country, pending_artist, in_list = None, None, False
    numbered = _is_numbered_list(text)
    rows = []
    for raw in (text or "").split("\n"):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if SONGS_HEADER.match(line):
            in_list = True
            continue
        # Furniture first, THEN the flag — "1. 🇬🇧 (United Kingdom) Steve
        # Winwood - While You See A Chance" files the country behind the list
        # number, and looking only at the start of the line found neither.
        line = ITEM_NUM.sub("", line).strip()
        if numbered:
            line = BARE_NUM.sub("", line).strip()
        c, line = _strip_country_prefix(line)
        if c:
            country, pending_artist, in_list = c, None, False
        if not line:
            continue                     # the line was only a country header
        if SONGS_HEADER.match(line):
            in_list = True
            continue
        if not line:
            continue
        parts = SPLIT.split(line, maxsplit=1)
        if len(parts) >= 2:
            artist, track = parts[0].strip(" .·|"), parts[1].strip(" .·|")
            in_list = False
        elif in_list and pending_artist:
            artist, track = pending_artist, line
        else:
            # No dash and no list open: this is prose, or the artist header
            # that a `Songs:` block below will refer back to. Remember it only
            # if it reads like a name — a whole sentence is not one.
            if len(line) <= 60 and not line.endswith((".", "!", "?", ":")):
                pending_artist = line.strip(" .·|")
            continue
        track, year = _split_year(track)
        artist = DECOR_TAIL.sub("", artist).strip(" .·|")
        if not artist or not track or len(artist) > 90 or len(track) > 120:
            continue
        if not _plausible(artist, track):
            continue
        rows.append({
            "raw": f"{artist} - {track}",
            "artist": artist, "track": track,
            "label": None, "year": year, "country": country,
            "style": None,
        })
        pending_artist = artist
    return rows


def _get(url, data=None):
    body = urllib.parse.urlencode(data).encode() if data else None
    headers = dict(HEADERS)
    if body is not None:
        tok = _prime()
        if tok:
            headers["x-csrftoken"] = tok
        headers["X-Requested-With"] = "XMLHttpRequest"
        headers["Referer"] = "https://www.instagram.com/"
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    req = urllib.request.Request(url, data=body, headers=headers)
    try:
        with _OPENER.open(req, timeout=30) as r:
            return json.load(r), None
    except urllib.error.HTTPError as e:
        return None, f"HTTP {e.code}: {e.read()[:120].decode('utf8', 'replace')}"
    except Exception as e:
        return None, str(e)[:120]


def profile(handle):
    d, err = _get(PROFILE.format(u=urllib.parse.quote(handle)))
    if err:
        return None, err
    u = (d.get("data") or {}).get("user")
    if not u:
        return None, "no user in response"
    return u, None


def _caption_of(node):
    edges = ((node.get("edge_media_to_caption") or {}).get("edges")) or []
    if edges:
        return (edges[0].get("node") or {}).get("text") or ""
    cap = node.get("caption")
    if isinstance(cap, dict):
        return cap.get("text") or ""
    return cap or ""


def captions(handle, limit=None, verbose=True):
    """Every caption on the account, newest first. Returns (list, error)."""
    u, err = profile(handle)
    if err:
        return [], err
    uid = u.get("id")
    out = []
    # NOT seeded from the profile response: its 12 newest posts are the same 12
    # that clips page 1 returns, and counting both reported 59 captions for 47
    # actual posts — a yield number that flatters itself by a quarter.
    max_id, page = None, 0
    while True:
        if limit and len(out) >= limit:
            break
        time.sleep(PACE_S)
        form = {"target_user_id": str(uid), "page_size": "12",
                "include_feed_video": "true"}
        if max_id:
            form["max_id"] = max_id
        d, err = _get(CLIPS, form)
        if err:
            # Stop, don't retry: IG's throttle answers "wait a few minutes",
            # and hammering it is how an account-less scrape becomes a blocked
            # IP for the rest of the day.
            if verbose:
                print(f"    (stopped after {len(out)} captions: {err})")
            break
        items = d.get("items") or []
        if not items:
            break
        for it in items:
            media = (it.get("media") or it)
            out.append(_caption_of(media))
        page += 1
        pi = d.get("paging_info") or {}
        max_id = pi.get("max_id")
        if verbose:
            print(f"    page {page}: +{len(items)} (total {len(out)})")
        if not pi.get("more_available") or not max_id:
            break
    return [c for c in out if c], None


def _plausible(artist, track):
    """Is this an artist/track pair, or a sentence that happened to hold a dash?

    Measured on the 2026-08-05 sweep: fish56octagon's 2,768 DJ posts produced
    634 "candidates" of which ~98% were prose cut at a dash — "Techno in its
    original and purest form" / "Detroit Techno #techno". The other three
    accounts scored 95-97% under these same rules, so this rejects the noise
    without touching the signal.

    Hashtags and @handles are the giveaway: a curator writing a tracklist does
    not put them inside a title, and a curator writing a caption almost always
    does.
    """
    for f in (artist, track):
        if "#" in f or "@" in f:
            return False
    # 16, not 12: a real Cole Pulice title runs to 14 words. The hashtag
    # and verb rules do the discriminating here; length is only a backstop.
    if len(artist.split()) > 6 or len(track.split()) > 16:
        return False
    if artist.endswith(("!", "?", ":", ",")):
        return False
    # A dash inside a sentence usually leaves a verb on the left-hand side.
    if re.search(r"\b(is|are|was|were|has|have|will|can|gonna)\b", artist, re.I):
        return False
    return True


def parse_caption(caption):
    """A caption -> LIST of {artist, track, label, year, country, style}.

    A list, because one post can name a dozen tracks. Four grammars, tried in
    order of how much they yield: a `Tracks mentioned:` block (many), a
    flag-headed country block (many, WITH the country), a structured
    `Artist – Track  Label : …` line (one, with metadata), and prose naming a
    quoted title (one).
    """
    text = (caption or "").strip()
    if not text:
        return []
    m = LIST_HEADER.search(text)
    if m:
        rows = []
        for line in text[m.end():].split("\n"):
            line = line.strip(" .·|-–—")
            if not line or line.startswith("#") or len(line) < 5:
                continue
            # A trailing "and more", a hashtag block, or prose resumes: stop at
            # the first line that is not Artist - Track rather than guessing.
            parts = SPLIT.split(line, maxsplit=1)
            if len(parts) < 2:
                continue
            a, t = parts[0].strip(" .·|"), parts[1].strip(" .·|")
            if a and t and len(a) <= 90 and len(t) <= 120 and _plausible(a, t):
                rows.append({"raw": f"{a} - {t}", "artist": a, "track": t,
                             "label": None, "year": None, "country": None,
                             "style": None})
        if rows:
            return rows
    # Before every single-row grammar below, because those read only the head
    # and a flag-filed caption's head is usually the joke ("Best Songs of All
    # Time: Part 94") with the citations underneath.
    #
    # The gate is a flag that LEADS a line, not a flag anywhere. A TRAILING
    # flag is the sleeve grammar's own anchor — "Colourful Environment –
    # Gboyega Adelaja 🇳🇬" is track-first with a decorated tail, and reading
    # it here returned it artist-first with the flag still inside the title.
    # Leading is what distinguishes "filed under a country" from "decorated
    # with one", and only the first is this grammar's claim.
    if _leads_with_a_flag(text):
        rows = _parse_flagged_block(text)
        if rows:
            return rows
    # The sleeve grammar before the generic dash split: its bracketed year is a
    # stronger claim to being a citation than a dash is, and the dash rule
    # reads these captions confidently in the wrong order.
    sleeve = _parse_sleeve(text)
    if sleeve:
        return [sleeve]
    # Before the generic dash split, because that split reads a release
    # headline confidently and wrongly: "HABIBI FUNK 019 - FERKAT AL ARD:" came
    # back as a track called FERKAT AL ARD by an artist called HABIBI FUNK 019.
    # Its gate — a headline colon plus a year or catalogue number, minus label
    # admin — is narrow enough to sit here without touching the curators whose
    # captions open with a citation.
    rel = _parse_release_headline(text, label_hint=_LABEL_HINT)
    if rel:
        return [rel]
    one = _parse_single(text)
    return [one] if one else []


def _parse_single(caption):
    """A caption -> {artist, track, label, year, country, style} or None.

    The head is whatever precedes the first `Label :`-style field, and inside
    that head the first dash separates artist from track. Curators who write no
    fields still land here through the same split, which is why the fallback is
    the same code path rather than a second parser to keep in step.
    """
    text = (caption or "").strip()
    if not text:
        return None
    fields = {}
    m = FIELD.search(text)
    head = text[:m.start()] if m else text.split("\n")[0]
    if m:
        # Walk the remaining `key : value` pairs. A value ends where the next
        # key begins, so the labels themselves are the delimiters.
        rest = text[m.start():]
        keys = list(FIELD.finditer(rest))
        for i, k in enumerate(keys):
            end = keys[i + 1].start() if i + 1 < len(keys) else len(rest)
            name = k.group(1).lower()
            val = rest[k.end():end].strip().strip(",;|").strip()
            if val:
                fields[name] = " ".join(val.split())[:120]
    head = " ".join(head.replace("\n", " ").split())
    # The curator's own list furniture, stripped here as well as in the
    # flag-block grammar: "album recommendation: The Underdark - …" shipped an
    # artist called "album recommendation: The Underdark", which resolves to
    # nothing. Leading-only and separator-anchored, so a title is never cut.
    head = ITEM_NUM.sub("", head).strip()
    # …and the country, for the same reason: "🇬🇧 Boom Devil – Cut Loose" and
    # "🇬🇧 (United Kingdom) Steve Winwood – While You See A Chance" reach this
    # fallback when nothing above claims them, and the dash split hands the
    # whole prefix over as the artist name.
    lead_country, head = _strip_country_prefix(head)
    if not head:
        return None
    # A QUOTED title is a stronger signal than a dash and is tried first. The
    # other order cost 2700audit a row: "083/2700 submitted by danluck from
    # 2700vault - collaborative playlist made by the community" has a dash in
    # it, so the dash rule claimed the whole sentence before the quoted title
    # two lines down was ever looked at.
    # QUOTED first, then the indexed line. The quotes are explicit about where
    # the title ends, which the index is not: "083/2700 submitted by danluck
    # from 2700vault" gave NUMBERED_BY the pair ("submitted", "danluck") while
    # the actual citation sat quoted on the next line. Quotes also delimit the
    # capture, so the title comes out clean instead of carrying the marks.
    q = QUOTED_BY.search(text) or NUMBERED_BY.search(text)
    if q:
        # QUOTED_BY carries one title group per delimiter pair, all but one of
        # them None; NUMBERED_BY carries a single pair. Either way the title is
        # the first group that matched and the artist is the last.
        got = [g for g in q.groups() if g is not None]
        track = got[0].strip(" .·|")
        artist = ARTIST_TAIL.sub("", got[-1]).strip(" .·|@,;")
    else:
        parts = SPLIT.split(head, maxsplit=1)
        if len(parts) < 2:
            return None
        artist, track = parts[0].strip(" .·|"), parts[1].strip(" .·|")
    if not artist or not track or len(artist) > 90 or len(track) > 120:
        return None
    if not _plausible(artist, track):
        return None
    def pick(*names):
        for n in names:
            for k, v in fields.items():
                if k.startswith(n):
                    return v
        return None
    return {
        "raw": f"{artist} - {track}",
        "artist": artist, "track": track,
        "label": pick("label"),
        "year": (pick("ann", "year") or "")[:12] or None,
        # A named field is a stronger claim than a flag — the curator wrote it
        # out — so the flag only fills the gap.
        "country": (pick("localisation", "location", "pays", "country")
                    or lead_country),
        "style": pick("style", "genre"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("handles", nargs="+")
    ap.add_argument("--limit", type=int, help="max captions per handle")
    ap.add_argument("--out", help="write candidates JSON (otherwise dry-run only)")
    args = ap.parse_args()

    global _LABEL_HINT
    all_rows, mentions, labels = [], {}, {}
    for h in args.handles:
        _LABEL_HINT = h
        print(f"\n=== @{h} ===")
        caps, err = captions(h, limit=args.limit)
        if err:
            print(f"  FAILED: {err}")
            continue
        rows = []
        for c in caps:
            for u in MENTION.findall(c):
                if u.lower() != h.lower():
                    mentions[u] = mentions.get(u, 0) + 1
            for p in parse_caption(c):
                p["source"] = f"ig:{h}"
                rows.append(p)
                if p["label"]:
                    labels[p["label"]] = labels.get(p["label"], 0) + 1
        # Resolve the sleeve grammar's ambiguous half from this curator's own
        # explicit posts. "X by Y" says which side is the artist; "X – Y" does
        # not, and the two are the same curator writing the same thing. If they
        # never once wrote "by", nothing is swapped — the house convention
        # (artist first) stands rather than a guess being applied to 80 rows.
        told = [r for r in rows if r.get("orient") == "track-first"]
        # Settled the other way — a quoted title on the right — so already in
        # house order and not to be swapped with the rest.
        held = [r for r in rows if r.get("orient") == "artist-first"]
        ambiguous = [r for r in rows if "orient" in r and r["orient"] is None]
        swap = list(told)
        for r in told + held:
            r["orient"] = "stated"
        if told and ambiguous:
            print(f"  orientation: {len(told)} post(s) say \"<track> by <artist>\" "
                  f"— reading {len(ambiguous)} dash post(s) the same way")
            swap += ambiguous
            for r in ambiguous:
                # NOT settled, only defaulted. This curator is inconsistent —
                # "Colourful Environment – Gboyega Adelaja" is track-first while
                # "Gino Paoli – La Gatta" and "Kourosh Yaghmaei – Asheghaneh" are
                # not — so the majority reading is applied and MARKED, and
                # resolve_curator_artists.py settles each row against MusicBrainz
                # before anything is written to the pool.
                r["orient"] = "assumed"
        elif ambiguous:
            print(f"  orientation: {len(ambiguous)} dash post(s) and no \"by\" post "
                  f"to learn from — left as artist-first")
            for r in ambiguous:
                r["orient"] = "as-written"
        for r in swap:
            r["artist"], r["track"] = r["track"], r["artist"]

        yielded = sum(1 for c in caps if parse_caption(c))
        pct = (100.0 * yielded / len(caps)) if caps else 0
        print(f"  captions {len(caps)}  ->  {len(rows)} candidates from "
              f"{yielded} posts ({pct:.0f}% of posts yielded)")
        with_country = sum(1 for r in rows if r["country"])
        with_style = sum(1 for r in rows if r["style"])
        print(f"  with country {with_country}   with style {with_style}   "
              f"with label {sum(1 for r in rows if r['label'])}")
        for r in rows[:6]:
            extra = " · ".join(x for x in (r["country"], r["year"], r["style"]) if x)
            print(f"    {r['artist']} — {r['track']}" + (f"   [{extra[:60]}]" if extra else ""))
        all_rows.extend(rows)

    print(f"\nTOTAL parsed candidates: {len(all_rows)}")
    uniq = {r["raw"].lower(): r for r in all_rows}
    print(f"unique: {len(uniq)}")
    if mentions:
        top = sorted(mentions.items(), key=lambda kv: -kv[1])[:15]
        print("\ncurator leads (@mentions, the only expansion graph available "
              "unauthenticated):")
        print("  " + ", ".join(f"{u}×{n}" for u, n in top))
    if labels:
        top = sorted(labels.items(), key=lambda kv: -kv[1])[:12]
        print("\nlabels named most (a second expansion axis — many are on Bandcamp):")
        print("  " + ", ".join(f"{l}×{n}" for l, n in top))
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(list(uniq.values()), fh, ensure_ascii=False, indent=1)
        print(f"\nwrote {len(uniq)} candidates -> {args.out}")
    else:
        print("\n(dry run — nothing written; pass --out to save)")


if __name__ == "__main__":
    sys.exit(main() or 0)
