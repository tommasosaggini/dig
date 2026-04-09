#!/usr/bin/env python3
"""
DIG — One-time genre bootstrap from Wikipedia + musicgenreslist.com scraping.

Merges all scraped genres, deduplicates, filters out non-searchable terms,
and writes new genres to the genres table (previously discovered_genres.json).
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR = ROOT
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# ── All scraped genres from Wikipedia + musicgenreslist.com ──
# Sources:
#   - https://en.wikipedia.org/wiki/List_of_music_genres_and_styles
#   - https://en.wikipedia.org/wiki/List_of_electronic_music_genres
#   - https://en.wikipedia.org/wiki/List_of_rock_genres
#   - https://en.wikipedia.org/wiki/List_of_jazz_genres
#   - https://en.wikipedia.org/wiki/List_of_hip_hop_genres
#   - https://en.wikipedia.org/wiki/Music_of_Africa
#   - https://en.wikipedia.org/wiki/Latin_music_(genre)
#   - https://www.musicgenreslist.com/

SCRAPED = [
    # ── Electronic (Wikipedia) ──
    "ambient dub", "dark ambient", "ambient industrial", "dungeon synth",
    "isolationism", "dreampunk", "illbient", "space music", "bass music",
    "brostep", "post-dubstep", "reggaestep", "riddim", "kawaii future bass",
    "jungle terror", "midtempo bass", "uk bass", "wave", "hardwave",
    "acid breaks", "baltimore club", "jersey club", "philly club",
    "breakbeat hardcore", "darkcore", "hardcore breaks", "broken beat",
    "florida breaks", "nu skool breaks", "progressive breaks",
    "psychedelic breakbeat", "psybient", "psydub", "trip rock",
    "afro cosmic music", "electro-disco", "hi-nrg", "eurodance",
    "italo dance", "spacesynth", "space disco", "eurodisco", "nu-disco",
    "post-disco", "boogie", "pop kreatif", "darkstep", "drumfunk",
    "drumstep", "hardstep", "intelligent drum and bass",
    "atmospheric drum and bass", "jazzstep", "jump-up", "liquid funk",
    "neurofunk", "sambass", "techstep", "dub poetry", "dance-rock",
    "alternative dance", "baggy", "new rave", "dance-punk", "freestyle",
    "disco polo", "sophisti-pop", "electroclash", "electropop",
    "wonky pop", "cold wave", "dark wave", "neoclassical dark wave",
    "neue deutsche todeskunst", "ethereal wave", "nu-gaze", "minimal wave",
    "neue deutsche welle", "new romantic", "synth-metal", "electrogrind",
    "electronicore", "synth-punk", "folktronica", "live electronic",
    "livetronica", "laptronica", "nu jazz", "jazztronica",
    "progressive electronic", "berlin school", "kosmische musik",
    "asian underground", "afrobeats", "azonto", "coupé-décalé",
    "shangaan electro", "changa tuki", "dancehall pop", "denpa music",
    "guaracha", "funk carioca", "funk melody", "funk ostentação",
    "proibidão", "rasteirinha", "merenhouse", "nortec", "rabòday",
    "rara tech", "russ music", "shamstep", "tecnocumbia",
    "tribal guarachero", "manila sound", "black midi",
    "deconstructed club", "electroacoustic music", "acousmatic music",
    "electroacoustic improvisation", "soundscape", "microsound",
    "danger music", "japanoise", "harsh noise", "harsh noise wall",
    "death industrial", "power noise", "plunderphonics", "sampledelia",
    "reductionism", "lowercase", "onkyokei", "funktronica", "synth-funk",
    "hard nrg", "dubstyle", "euphoric frenchcore", "euphoric hardstyle",
    "rawstyle", "trapstyle", "jumpstyle", "lento violento", "mákina",
    "bouncy techno", "raggacore", "digital hardcore", "frenchcore",
    "early hardcore", "mainstream hardcore", "uk hardcore",
    "industrial hardcore", "j-core", "extratone", "flashcore",
    "splittercore", "hauntology", "hypnagogic pop", "darksynth",
    "sovietwave", "future funk", "hardvapour", "mallsoft", "afroswing",
    "hipster hop", "cloud rap", "crunkcore", "snap music", "glitch hop",
    "lofi hip-hop", "miami bass", "mumble rap", "afro trap",
    "brooklyn drill", "uk drill", "drift phonk", "brazilian phonk",
    "plugg", "uk trap", "afro house", "afro tech", "kidandali",
    "ambient house", "balearic beat", "ballroom", "bass house",
    "brazilian bass", "slap house", "blog house", "chicago hard house",
    "disco house", "diva house", "hardbag", "big room house",
    "future rave", "complextro", "dutch house", "fidget house",
    "melbourne bounce", "electro swing", "eurohouse", "french house",
    "funky house", "future house", "garage house", "ghetto house",
    "ghettotech", "juke house", "hip house", "electro hop",
    "italo house", "jackin house", "jazz house", "latin house",
    "melodic house", "microhouse", "moombahcore", "moombahton",
    "moombahsoul", "new jersey sound", "outsider house", "lo-fi house",
    "soulful house", "stadium house", "tribal house", "trouse",
    "uk hard house", "pumping house", "hardbass", "scouse house",
    "dark electro", "aggrotech", "electronic body music", "futurepop",
    "industrial hip-hop", "cyber metal", "neue deutsche härte",
    "martial industrial", "witch house", "algorave", "drill n bass",
    "ragga jungle", "alternative r&b", "acid techno", "ambient techno",
    "birmingham sound", "bleep techno", "detroit techno", "dub techno",
    "hard techno", "free tekno", "jungletek", "raggatek",
    "industrial techno", "schaffel", "toytown techno", "acid trance",
    "balearic trance", "dream trance", "eurotrance", "hands up",
    "nitzhonot", "hard trance", "progressive trance", "dark psytrance",
    "full-on", "minimal psytrance", "progressive psytrance",
    "suomisaundi", "tech trance", "uplifting trance", "vocal trance",
    "2-step garage", "bassline", "breakstep", "future garage",
    "grindie", "speed garage", "uk funky", "funkstep", "wonky",
    "chiptune", "bitpop", "skweee", "nintendocore",
    "chopped and screwed", "disco edits", "nightcore", "tecno brega",
    "rave music",

    # ── Rock (Wikipedia) ──
    "acid rock", "acoustic rock", "afro-punk", "afro rock",
    "anatolian rock", "anti-folk", "arabic rock", "arena rock",
    "art punk", "art rock", "avant-funk", "avant-garde metal",
    "avant-prog", "bakersfield sound", "beat music", "blackgaze",
    "blackened crust", "blackened death-doom", "blackened death metal",
    "black-doom", "blackened grindcore", "black n roll",
    "blackened thrash metal", "blues rock", "boogie rock", "britpop",
    "c86", "cambodian rock", "canterbury sound", "celtic fusion",
    "celtic punk", "celtic metal", "celtic rock", "chicano rock",
    "cock rock", "college rock", "comedy rock", "country metal",
    "cowpunk", "crabcore", "crack rock steady", "crossover thrash",
    "d-beat", "death n roll", "deathcore", "death-doom", "deathgrind",
    "deathrock", "djent", "drone metal", "dunedin sound", "egg punk",
    "electric blues", "emo pop", "emo rap", "emo revival", "emoviolence",
    "epic doom metal", "experimental rock", "flamenco rock",
    "folk black metal", "folk metal", "folk punk", "freakbeat",
    "funk metal", "funk rock", "garage punk", "glam metal", "glam punk",
    "glam rock", "goregrind", "gothabilly", "gothic country",
    "gothic-doom", "gothic metal", "gothic rock", "grebo", "grunge",
    "gypsy punk", "hard rock", "heartland rock", "heavy metal",
    "horror punk", "indie folk", "indie pop", "indie rock",
    "indie surf", "indorock", "instrumental rock", "jangle pop",
    "jam rock", "jazz fusion", "jazz metal", "jazz rock", "kawaii metal",
    "krautrock", "la movida madrileña", "landfill indie",
    "latin alternative", "latin metal", "latin rock", "madchester",
    "math rock", "melodic black-death", "melodic death metal",
    "melodic hardcore", "melodic metalcore", "melodic punk",
    "midwest emo", "mod revival", "música popular brasileira",
    "nagoya kei", "neofolk", "neo-rockabilly", "neo-psychedelia",
    "new weird america", "no wave", "noise pop", "noise rock",
    "nu metal", "occult rock", "oi!", "pagan metal", "pagan rock",
    "paisley underground", "pirate metal", "pop punk", "pop rock",
    "post-black metal", "post-britpop", "post-grunge", "post-hardcore",
    "post-metal", "post-punk revival", "power pop", "power metal",
    "progressive doom", "progressive metal", "progressive metalcore",
    "progressive punk", "proto-punk", "psychedelic folk",
    "psychedelic funk", "psychedelic soul", "psychobilly",
    "pub rock", "punk blues", "punk funk", "punk jazz", "punk rap",
    "queercore", "raga rock", "rapcore", "rap metal", "rap rock",
    "red dirt", "riot grrrl", "rock and roll", "rocksteady",
    "roots rock", "sadcore", "screamo", "shoegaze", "shock rock",
    "ska punk", "skate punk", "skiffle", "slacker rock", "sludge metal",
    "southern rock", "space rock", "speed metal", "stoner rock",
    "street punk", "sufi rock", "surf music", "surf punk", "swamp blues",
    "swamp pop", "swamp rock", "symphonic black metal",
    "symphonic death metal", "symphonic metal", "symphonic rock",
    "thrash metal", "trap metal", "tropicália", "twee pop",
    "viking metal", "visual kei", "yacht rock", "yé-yé", "zamrock",
    "zeuhl", "zoomergaze",

    # ── Jazz (Wikipedia) ──
    "afro-cuban jazz", "bebop", "big band", "cape jazz", "chamber jazz",
    "continental jazz", "cool jazz", "crossover jazz", "dixieland",
    "ethno jazz", "european free jazz", "flamenco jazz", "free funk",
    "gypsy jazz", "hard bop", "indo jazz", "jazz blues", "jazz-funk",
    "jazz pop", "jump blues", "kansas city jazz", "m-base", "marabi",
    "mainstream jazz", "modal jazz", "neo-bop jazz", "neo-swing",
    "jazz noir", "orchestral jazz", "post-bop", "samba-jazz", "ska jazz",
    "soul jazz", "spiritual jazz", "straight-ahead jazz", "stride jazz",
    "swing", "trad jazz", "west coast jazz",

    # ── Hip Hop (Wikipedia) ──
    "boom bap", "bounce", "road rap", "chopper", "dirty rap",
    "gangsta rap", "mafioso rap", "memphis rap", "hyphy",
    "frat rap", "hardcore hip hop", "political hip hop",
    "conscious hip hop", "slab music", "southern hip hop",
    "sigilkore", "krushclub", "pluggnb", "rage", "tread rap",
    "turntablism", "underground hip-hop", "country rap", "hip-hop soul",
    "hipdut", "rap opera", "bongo flava", "boomba music", "genge",
    "hip-hop galsen", "hipco", "hiplife", "igbo rap", "motswako",
    "zenji flava", "gyp-hop", "low bap", "cumbia rap",
    "urban pasifika", "old-school hip-hop", "golden age hip-hop",

    # ── Africa (Wikipedia) ──
    "makwaya", "mbube", "township music", "jùjú", "fuji",
    "jaiva", "afrofusion", "ndombolo", "makossa", "kizomba",
    "isicathamiya", "african pop", "afro-pop", "african rumba",
    "urban grooves",

    # ── Latin (Wikipedia) ──
    "latin pop", "latin r&b", "rock en español", "latin urban",
    "urbano music", "regional mexican", "tropical music", "pregón",
    "boogaloo", "salsa romantica", "latin ballad", "banda",
    "duranguense", "latin soul",

    # ── musicgenreslist.com additions ──
    "acoustic blues", "african blues", "blues shouter", "classic blues",
    "contemporary blues", "dark blues", "doom blues", "harmonica blues",
    "hill country blues", "hokum blues", "modern blues", "piano blues",
    "piedmont blues", "ragtime blues", "urban blues", "west coast blues",
    "zydeco", "cajun", "filk music", "freak folk", "industrial folk",
    "techno-folk", "australian country", "close harmony",
    "contemporary country", "country gospel", "country pop", "country soul",
    "cowboy", "dansband", "franco-country", "hellbilly music",
    "lubbock sound", "nashville sound", "progressive bluegrass",
    "sertanejo", "texas country", "traditional bluegrass",
    "traditional country", "truck-driving country", "urban cowboy",
    "bubblegum dance", "turbofolk", "doomcore", "terrorcore",
    "schranz", "dark psy", "psybreaks", "orchestral uplifting",
    "baila", "bhojpuri", "filmi", "indian pop", "indian ghazal",
    "lavani", "luk krung", "pinoy pop", "pop sunda", "ragini",
    "apala", "akpala", "bikutsi", "jit", "kapuka", "kwela",
    "lingala", "rumba lingala", "maloya", "marrabenta", "morna",
    "museve", "palm-wine", "sakara", "sega", "seggae", "semba",
    "zouglou", "fann at-tanbura", "fijiri", "khaliji", "liwa", "sawt",
    "chicha", "criolla", "joropo", "mariachi", "nuevo flamenco",
    "punta", "raíces", "timba", "twoubadou", "zouk",
    "axé", "brega", "frevo", "lambada", "maracatu", "pagode",
    "zouk-lambada", "entechno", "laïkó", "levenslied",
    "austropop", "balkan pop", "russian pop", "iranian pop",
    "turkish pop", "vispop", "arab pop", "space age pop",
    "a cappella", "barbershop", "vocal jazz", "vocal pop", "yodel",
    "carolina beach music", "motown", "modern soul", "southern soul",
    "piphat", "compas", "méringue", "chutney soca",

    # ── Additional world/traditional not in seed ──
    "congolese rumba", "soukous", "son cubano", "bomba", "conga",
    "kaiso", "mugham", "min'yō", "kayōkyoku", "mor lam", "luk thung",
    "keroncong", "kundiman", "dikir barat", "baul", "rabindra sangeet",
    "chaabi", "dabke", "maqam", "cajun", "delta blues", "appalachian",
    "alpine folk", "balkan brass", "sevdalinka", "fado", "throat singing",
    "carnatic", "hindustani classical", "guqin", "erhu", "pipa",
    "persian classical", "ottoman classical", "arabic classical",
    "georgian polyphony", "corsican polyphony", "pygmy music",
    "tuvan throat singing", "overtone singing",
]

# ── Load existing GENRE_POOL from discover.py to know what's already seeded ──
SEED_GENRES = set()
# Hardcode the seed genres we know from discover.py
seed_lists = {
    "traditional": [
        "fado", "flamenco", "tango", "rebetiko", "enka", "qawwali", "ghazal",
        "gamelan", "gagaku", "pansori", "raï", "gnawa", "griot", "highlife",
        "mbalax", "benga", "taarab", "mbaqanga", "chimurenga", "calypso",
        "mento", "son jarocho", "huayno", "forró", "choro", "cueca",
        "joik", "sean-nós", "klezmer", "csárdás", "throat singing",
        "carnatic", "hindustani classical", "guqin", "erhu", "pipa",
        "min'yō", "kayōkyoku", "mor lam", "luk thung", "dangdut",
        "keroncong", "kundiman", "dikir barat", "bhangra", "baul",
        "rabindra sangeet", "chaabi", "dabke", "khaleeji", "maqam",
        "washboard", "cajun", "zydeco", "delta blues", "appalachian",
        "polka", "alpine folk", "balkan brass", "sevdalinka", "mugham",
    ],
    "electronic": [
        "techno", "house", "ambient", "drum and bass", "dubstep", "trance",
        "gabber", "breakcore", "idm", "glitch", "vaporwave", "synthwave",
        "electro", "acid house", "deep house", "minimal techno",
        "psytrance", "hardstyle", "future garage", "uk garage",
        "footwork", "juke", "gqom", "amapiano", "baile funk",
        "kuduro", "singeli", "mahraganat", "budots", "koplo",
        "new beat", "ebm", "industrial", "noise music", "power electronics",
        "dark ambient", "drone", "microsound", "granular",
        "space ambient", "ambient dub", "future bass", "tropical house",
        "progressive house", "chillwave",
    ],
    "rock": [
        "krautrock", "shoegaze", "post-punk", "noise rock", "math rock",
        "post-rock", "stoner rock", "doom metal", "black metal",
        "death metal", "grindcore", "powerviolence", "hardcore punk",
        "crust punk", "sludge metal", "prog rock", "psychedelic rock",
        "garage rock", "surf rock", "rockabilly", "new wave",
        "gothic rock", "ethereal wave", "coldwave", "dream pop",
        "indie rock", "emo", "screamo", "folk rock",
    ],
    "jazz_soul": [
        "free jazz", "ethio-jazz", "afrobeat", "latin jazz", "bossa nova",
        "samba", "mpb", "tropicália", "northern soul", "deep funk",
        "gospel", "spirituals", "doo-wop", "neo-soul", "quiet storm",
        "acid jazz", "fusion", "smooth jazz", "big band", "bebop",
        "cool jazz", "modal jazz", "avant-garde jazz",
        "r&b", "new jack swing", "contemporary r&b", "slow jams",
    ],
    "hip_hop": [
        "boom bap", "trap", "drill", "grime", "phonk", "lo-fi hip hop",
        "chopped and screwed", "crunk", "g-funk", "conscious hip hop",
        "abstract hip hop", "jazz rap", "cloud rap", "memphis rap",
        "uk hip hop", "french rap", "latin trap",
    ],
    "pop_experimental": [
        "art pop", "chamber pop", "baroque pop", "hyperpop", "pc music",
        "city pop", "cantopop", "mandopop", "j-pop", "k-pop",
        "italo disco", "eurobeat", "schlager", "chanson", "canzone napoletana",
        "musique concrete", "tape music", "field recordings",
        "spectral music", "microtonal", "just intonation",
        "bedroom pop", "singer-songwriter", "lo-fi indie",
    ],
    "reggae_caribbean": [
        "roots reggae", "dub", "dancehall", "ska", "rocksteady",
        "lovers rock", "ragga", "kompa", "soca", "chutney",
        "steelpan", "reggaeton", "dembow",
    ],
    "classical": [
        "baroque", "romantic era", "contemporary classical", "minimalism",
        "opera", "lieder", "choral", "sacred music", "gregorian chant",
        "gamelan composition", "gagaku composition",
        "musical theater", "broadway",
    ],
    "country_americana": [
        "country", "bluegrass", "honky-tonk", "outlaw country", "americana",
        "country blues", "western swing", "country rock", "alt-country",
        "country folk", "tejano", "norteño", "corridos", "ranchera",
    ],
    "latin": [
        "cumbia", "bachata", "vallenato", "merengue", "salsa",
        "bolero", "trova", "nueva canción", "latin rock",
        "boogaloo", "mambo", "cha-cha-chá", "rumba",
    ],
    "ambient_meditative": [
        "meditation music", "sound bath", "binaural", "nature sounds",
        "new age", "healing music", "tibetan bowls", "crystal bowls",
        "ambient folk", "slowcore", "sadcore", "funeral doom",
        "spoken word", "poetry", "sound poetry",
    ],
}

for genres in seed_lists.values():
    SEED_GENRES.update(g.lower() for g in genres)

# ── Load existing genres from DB ──
from lib.genres import load as db_load_genres, add as db_add_genres
existing = db_load_genres()  # set of lowercase strings
existing_lower = existing

# ── Deduplicate and filter ──
# Skip terms that aren't useful as Spotify search queries
SKIP_TERMS = {
    "popular music", "popular", "classical period", "art music",
    "early music", "progressive", "light music", "soundtrack",
    "film score", "video game music", "children's music", "lullabies",
    "sing-along", "stories", "comedy", "novelty", "stand-up comedy",
    "vaudeville", "commercial", "jingles", "tv themes", "holiday",
    "christmas", "easter", "halloween", "thanksgiving", "disney",
    "background", "elevator", "furniture", "middle of the road",
    "exercise", "fitness & workout", "karaoke", "inspirational",
    "instrumental", "march", "marching band", "spoken word",
    "wedding music", "holiday: other", "travel", "relaxation",
    "nature", "meditation", "healing", "environmental", "ccm",
    "praise & worship", "foreign cinema", "movie soundtrack",
    "musicals", "original score", "tv soundtrack", "classic",
    "rock music", "hip-hop", "hip-hop/rap", "rap", "r&b/soul",
    "r&b", "soul", "pop", "rock", "metal", "punk", "blues",
    "country", "folk", "jazz", "reggae", "classical", "electronic",
    "world", "dance", "asia", "africa", "europe", "australia",
    "south america", "north america", "middle east", "caribbean",
    "japan", "thailand", "germany", "austria", "sweden", "uk",
    "france", "greece", "portugal", "south africa", "hawaii",
    "brazil", "latino", "latin", "club", "club dance",
    "contemporary", "traditional", "modern", "classic",
    "new", "vocal", "standards", "love song",
    "anime", "easy listening", "bop",
    "chanukah", "christmas: children's", "christmas: classic",
    "christmas: classical", "christmas: comedy", "christmas: jazz",
    "christmas: modern", "christmas: pop", "christmas: r&b",
    "christmas: religious", "christmas: rock",
    "south / southeast asia", "tex-mex",
}

new_genres = []
seen = set()

for g in SCRAPED:
    g_clean = g.strip()
    g_lower = g_clean.lower()

    # Skip if empty, too short, or a skip term
    if len(g_clean) < 3:
        continue
    if g_lower in SKIP_TERMS:
        continue
    # Skip if already in seed pool or existing discovered
    if g_lower in SEED_GENRES:
        continue
    if g_lower in existing_lower:
        continue
    # Skip if already seen in this batch
    if g_lower in seen:
        continue

    seen.add(g_lower)
    new_genres.append(g_clean)

# Write new genres to DB
inserted = db_add_genres(new_genres, source="bootstrap") if new_genres else 0
total = len(existing) + inserted

print(f"\nDIG — GENRE BOOTSTRAP")
print(f"  Existing genres in DB: {len(existing)}")
print(f"  New genres from Wikipedia/musicgenreslist: {len(new_genres)} (inserted: {inserted})")
print(f"  Total genres in DB: {total}")
print(f"  Seed pool genres: {len(SEED_GENRES)}")
print(f"  GRAND TOTAL searchable genres: {len(SEED_GENRES) + total}")

# Show a sample
if new_genres:
    import random
    sample = random.sample(new_genres, min(20, len(new_genres)))
    print(f"\n  Sample new genres: {', '.join(sample)}")
