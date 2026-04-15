"""
DIG — Track quality filter.

Rejects non-music content (tutorials, covers, compilations, medleys)
and shallow regional keyword results (e.g. "New Nepali Song", "Japanese Version").

Used at ingest time by discover.py, discover_artists.py, discover_youtube.py.
"""

import re

# Always reject these patterns in track names
_ALWAYS_TRASH = re.compile(r'|'.join([
    r'\bevergreen\b.*\bsong', r'\bcompilation\b', r'\bcollection\b', r'\bnonstop\b',
    r'\bplaylist\b', r'\bjukebox\b', r'\bmegamix\b',
    r'\bmedley\b', r'\bbest of\b', r'\btop \d+\b',
    r'\bdj song\b', r'\bdj mix\b', r'\bdj remix\b',
    r'\breaction\b', r'\btutorial\b', r'\bintroduction to\b',
    r'\bhow to\b', r'\bexplained\b', r'\bdocumentary\b',
    r'\bcover\b(?!\w)',  # "cover" but not "discover", "recover"
    r'#music\s*$',
    # Wellness / frequency factory content
    r'^\d+(\.\d+)?\s*hz\b',           # "432 Hz ...", "0.5 Hz Delta Waves"
    r'\bbinaural\b', r'\bsolfeggio\b',
    r'\bwhite noise\b', r'\brain sounds?\b', r'\bnature sounds?\b',
    r'\bsleep sounds?\b', r'\bsound bath\b',
    r'\bcrystal bowl\b', r'\btibetan bowl\b',
    r'\bstress relief\b', r'\bdeep sleep\b', r'\bbrain waves?\b',
    r'\brelaxing ambient\b', r'\bambient (flow|calm|meditation|sleep)\b',
    r'\bmeditation (music|session|ocean|chillout)\b',
    r'\bhealing frequency\b', r'\bchakra\b', r'\breiki\b',
    r'\bspa music\b', r'\byoga music\b', r'\bzen music\b',
]), re.IGNORECASE)

# Wellness-factory artist names — regardless of track title
_WELLNESS_ARTISTS = re.compile(r'|'.join([
    r'soothing ambient', r'miracle tones', r'sacred society',
    r'dream supplier', r'ocean therapy', r'nature sound retreat',
    r'meditation spa', r'deep sleep music', r'erhu sleep',
    r'tranquility spa', r'oriental meditation music', r'wp sounds',
    r'big secret music', r'relaxation music', r'healing music',
    r'sleep music collective', r'frqncy', r'musicoterapia',
    r'sound bath.*music', r'music.*sound bath',
    r'432 hz', r'528 hz', r'639 hz', r'741 hz',
]), re.IGNORECASE)

# Regional demonym/language + generic descriptor = not a real song title
_REGIONAL = (
    r'(?:nepali|khmer|thai|vietnamese|cambodian|laotian|lao|burmese|myanmar|'
    r'tibetan|filipino|tagalog|indonesian|malay|japanese|korean|chinese|mongolian|'
    r'indian|hindi|bangla|bengali|pakistani|arabic|persian|turkish|kurdish|'
    r'african|ethiopian|nigerian|kenyan|brazilian|mexican|cuban|colombian|'
    r'peruvian|bolivian|chilean|argentine|russian|greek|serbian|romanian|'
    r'polish|hungarian|bulgarian|uzbek|kazakh|kyrgyz|tajik|turkmen|georgian|'
    r'armenian|hawaiian|polynesian|samoan|tongan|fijian|tamil|telugu|kannada|'
    r'malayalam|marathi|gujarati|punjabi|sinhalese|cebuano|javanese|sundanese|'
    r'balinese|malagasy|swahili|hausa|yoruba|igbo|amharic|oromo|somali|'
    r'zulu|xhosa|shona|tswana)'
)

_REGIONAL_TRASH = re.compile(
    r'(?:'
    + r'(?:new|old|classic|traditional|modern|popular|famous|beautiful|best|amazing|epic|emotional)\s+' + _REGIONAL
    + r'|' + _REGIONAL + r'\s+(?:version|song|music|dance|beat|remix|rap beat|folk|pop|rock|hip hop|traditional|classical|instrumental)'
    + r')',
    re.IGNORECASE
)


def is_trash(track_name, artist_name=""):
    """Return True if the track name or artist looks like non-music content."""
    if _ALWAYS_TRASH.search(track_name):
        return True
    if _REGIONAL_TRASH.search(track_name):
        return True
    if artist_name and _WELLNESS_ARTISTS.search(artist_name):
        return True
    return False
