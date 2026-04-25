"""
DIG — Track quality filter.

Rejects non-music content (tutorials, covers, compilations, medleys)
and shallow regional keyword results (e.g. "New Nepali Song", "Japanese Version").
Also rejects factory "ambient/lofi/wellness" content where the artist name
or track title is a genre/mood descriptor rather than a real act or song.

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
    # German wellness / white-noise stock titles
    r'\bbabyschlaf\b',
    r'\bwei[ßs]+e[rns]?\s+(rauschen|l[aä]rm)\b',
    r'\b(meeres|wind|regen|ozean|natur|wald)rauschen\b',
    r'\brauschen\s+(zum|f[uü]r)\s+(schlafen|einschlafen|entspannen|meditieren|baby)\b',
    # Kids / educational stock titles
    r'\babc\s+(alphabet|song|learning)\b',
    r'\blearn\s+with\s+\w+\b.{0,40}\b(abc|alphabet|numbers|colors|shapes|song)\b',
    r'\b(nursery|kids?)\s+(rhymes?|songs?|tunes?)\s+(for|vol\.?\s*\d+|compilation)',
    # Factory numbered-variant titles: genre-word + 2+ descriptor words +
    # explicit roman numeral. "Synthpop Tokyo Girl III", "Chillwave Beach
    # Cafe XIX". Requires 2+ middle words so legit short titles like
    # "Synthwave Dreamer III" don't trip.
    r'^(synthpop|synthwave|chillwave|chillstep|chillout|chill\s+out|vaporwave|'
    r'ambient|lofi|lo-fi|dreamwave|dubstep|deep\s+house)\s+'
    r'\S+\s+\S+(\s+\S+){0,2}\s+'
    r'(I{1,3}|IV|V|VI{1,3}|VII|VIII|IX|X{1,3}|XI{1,3}|XIV|XV|XVI{1,3}|XIX|XX{1,2})\s*$',
    r'\bcrystal bowls?\b', r'\btibetan bowls?\b', r'\bsinging bowls?\b',
    r'\bquartz\s+(crystal|bowls?)\b',
    r'\bguided\s+(meditation|relaxation|sleep|visualization)\b',
    r'\batmospheric\s+(horror|drone|dark|dungeon|ritual)\b',
    r'\b(distant|eerie|haunting|creepy|spooky)\s+(screams?|whispers?|voices?|echoes?|howls?)\b',
    r'\bstress relief\b', r'\bdeep sleep\b', r'\bbrain waves?\b',
    r'\brelaxing ambient\b', r'\bambient (flow|calm|meditation|sleep)\b',
    r'\bmeditation (music|session|ocean|chillout)\b',
    r'\bhealing frequency\b', r'\bchakra\b', r'\breiki\b',
    r'\bspa music\b', r'\byoga music\b', r'\bzen music\b',
    # Generic "[instrument] + vibe/descriptor" titles — not real song names
    # e.g. "Erhu Eternal Night Serenity", "Guqin Ambient Drift", "Gamelan Relaxation"
    r'\b(erhu|guqin|koto|shamisen|pipa|dizi|hulusi|shakuhachi|sitar|tabla|'
    r'gamelan|didgeridoo|mbira|balafon|kora|oud|saz|gayageum|haegeum|kalimba)\b'
    r'.{0,40}\b(instrumental|meditation|relax(ation)?|ambient|drift|flow|journey|'
    r'serenity|bliss|harmony|tranquil|healing|soothing|peaceful|dreamscape|'
    r'bamboo groove|night bloom|eternal|soundscape)\b',
    # Pure genre/mood word as the entire track title (with optional generic noun)
    # e.g. "Ambient Instrumental Drift", "Chillout Reflections", "Lofi Beats Vol 2",
    #      "Ambient Aura", "Chillwave Nebula", "Vaporwave Vibes"
    r'^(ambient|chillout|chill out|chillwave|chillstep|lofi|lo-fi|new age|'
    r'space ambient|dark ambient|vaporwave|synthwave)'
    r'\s+(instrumental|drift|flow|bliss|aura|aurora|nebula|cosmos|galaxy|haze|'
    r'waves?|tides?|echoes?|reflections?|soundscapes?|beats?|vibes?|music|'
    r'moods?|pads?|textures?|layers?|atmospheres?|vol\.?\s*\d+)\s*$',
    # Bare genre word as the entire title (no descriptor after). These are
    # almost always low-effort keyword-stuffed releases by stock artists —
    # "Ambient", "VAPORWAVE", "Synthwave", "Chillwave". Real artists with a
    # track actually named after a genre are rare; losing those is acceptable.
    r'^(ambient|chillwave|chillstep|chillout|chill out|lofi|lo-fi|new age|'
    r'space ambient|dark ambient|vaporwave|synthwave)\s*$',
    # "[Culture] [genre] Style/Version/Mix" — stock genre-imitation tracks
    # e.g. "Brazilian Jazz Bossa Nova Style", "Italian Lounge Jazz Mix"
    r'^(brazilian|mexican|italian|greek|spanish|portuguese|french|german|'
    r'turkish|russian|cuban|argentinian|colombian|peruvian|chilean|caribbean|'
    r'jamaican|african|latin)\s+'
    r'(jazz|bossa|samba|flamenco|tango|classical|traditional|instrumental|'
    r'folk|pop|blues|lounge|reggae|rumba|salsa|cumbia|mambo)'
    r'(\s+\w+){0,3}?\s+(style|version|mix|vibes?|mode|edition)\b',
    # "[culture/genre adjective] + [generic descriptor]" as full title
    # e.g. "Chinese Classical Instrumental", "Balinese Gamelan Meditation"
    r'^(chinese|japanese|korean|indian|balinese|tibetan|arabic|persian|celtic|'
    r'african|nordic|andean|gamelan|latin|tropical)\s+'
    r'(classical|traditional|instrumental|ambient|meditation|relaxation|folk|'
    r'music|lullaby|chant|drums?)\s*(music|vol\.?\s*\d+)?\s*$',
    # "[Culture] [imagery] [mood/state]" factory titles
    # e.g. "Chinese Plum Rain Meditation", "Chinese Temple Stillness",
    #      "Chinese Morning Mist", "Chinese Moonlit Path on Huangshan Peaks"
    r'^(chinese|japanese|korean|indian|balinese|tibetan|arabic|persian|'
    r'celtic|african|thai|vietnamese|mongolian|turkish|oriental|eastern)\s+'
    r'\S.{0,35}\b(meditation|serenity|stillness|flow|reflection|tranquil|'
    r'peace|moonlit|blossom|mist|dawn|serenade|reverie|whispers?|solitude|repose|'
    r'silence|awakening|tranquility|contemplation)\s*$',
    # "No copyright" in title — stock/factory content
    r'\bno[- ]copyright\b',
    # Pure instrument description as title: "Erhu Music", "Guzheng & Erhu Duet"
    r'^(erhu|guqin|guzheng|pipa|qin|koto|sitar|tabla|shamisen)\s+(music|&|and)\b',
    # Spa/massage/yoga + generic descriptor
    r'^(spa|massage|yoga|zen|reiki)\s+(ambient|music|soundscapes?|relaxation|instrumental|beats?)',
    # "Sexy/Sensual + genre" titles — stock background music
    r'\b(sexy|sensual|erotic)\s+(latino?|jazz|lounge|bossa|piano|guitar|sax|smooth)\b',
    # "X for sleep/plants/babies/dogs/cats/reading/studying" — functional stock content
    r'\bfor\s+(sleep|sleeping|plants|babies|dogs|cats|pets|studying|working|focus|'
    r'relaxation|meditation|yoga|massage|reading|concentration|driving|running|'
    r'cooking|cleaning)\b',
    # "For [wellness] and [wellness]" — pair-of-wellness-words, distinctive
    # enough to avoid false positives like "Atoms For Peace" (real band)
    r'\bfor\s+(sleep|sleeping|healing|peace|harmony|bliss|meditation|relaxation|'
    r'serenity|calm|tranquility|mindfulness|spirit|wellness|anxiety|stress)\s+'
    r'(and|&)\s+'
    r'(sleep|sleeping|healing|peace|harmony|bliss|meditation|relaxation|'
    r'serenity|calm|tranquility|mindfulness|spirit|wellness|anxiety|stress)\b',
    # "Chants for X" — stock devotional/wellness
    r'\b(chants?|mantras?|prayers?)\s+for\s+(sleep|healing|peace|relaxation|meditation)\b',
    # Throat singing / overtone singing pitched as wellness product
    r'\bthroat\s+(singing|chanting)\b.{0,40}\b(peace|healing|meditation|relaxation|'
    r'harmony|calm|serenity|tranquil|wellness|sleep|bliss|stress\s*relief)\b',
    r'\bover[- ]?ton(e|es|ing)\s+(singing|chanting|meditation|healing|practice)\b',
    # Stand-up comedy / non-music content
    r'\b(stand[- ]?up|spoken\s+word|monologue)\s+(comedy|special|routine|album|set|show|hour|tour)\b',
    r'\bcomedy\s+(club|special|album|show|hour|set|central|routine|tour|night\s+live)\b',
    r'\blive\s+(at|from)\s+(the\s+)?(apollo|improv|comedy\s+(club|cellar|store)|'
    r'laugh\s+factory|caroline\'?s)\b',
    # "Sound of [instrument/nature]" — descriptor-as-title
    r'^sound(s|scape|scapes)?\s+of\s+(the\s+)?(erhu|guqin|guzheng|pipa|koto|sitar|'
    r'tabla|shamisen|dizi|hulusi|shakuhachi|gayageum|haegeum|kora|oud|saz|'
    r'kalimba|mbira|balafon|didgeridoo|gamelan|bagpipes?|panpipes?|flute|harp|'
    r'drums?|rain|ocean|forest|nature|wind|waves?|birds?)\b',
    # "Relaxing [Culture] Ambient/Instrumental/Sounds" — factory compilation style
    r'\brelax(ing|ation)?\s+(chinese|japanese|korean|indian|balinese|tibetan|'
    r'arabic|persian|celtic|african|thai|vietnamese|mongolian|turkish|oriental|'
    r'asian|eastern|nordic|andalus(ian)?|spanish|flamenco|mediterranean|tropical|'
    r'latin|brazilian)\s+(ambient|instrumental|music|sounds?|tones?|meditation|'
    r'flow|traditional|folk)\b',
    # Nature field-recording SEO titles. Requires BOTH a nature-scene noun AND
    # a field-recording participle/adjective nearby (distant/feeding/chirping/
    # falling/soothing/etc.). Real song titles with one nature word are common;
    # pairing with a field-recording descriptor is the stock-content signature.
    # e.g. "Waves Near the Harbour with Distant Feeding Gulls"
    r'\b(waves?|ocean|seas?|rivers?|streams?|rains?|storms?|thunder|winds?|'
    r'forest|jungle|meadow|harbours?|harbors?|beach|shores?|surfs?|gulls?|birds?|'
    r'birdsong|whales?|crickets?|frogs?|wolves?|dolphins?|owls?|sparrows?|'
    r'raindrops?|droplets?|canopy|mist|fog|dew|breeze|tides?|glade|brook)\b'
    r'.{10,60}'
    r'\b(distant|distantly|feeding|calling|flocking|chirping|chirps?|howling|'
    r'rustling|whispering|crashing|trickling|trickle|dripping|pattering|'
    r'soothingly|droplet\s+of)\b',
    r'\b(distant|distantly|feeding|calling|flocking|chirping|chirps?|howling|'
    r'rustling|whispering|crashing|trickling|trickle|dripping|pattering|'
    r'soothingly|droplet\s+of)\b'
    r'.{10,60}'
    r'\b(waves?|ocean|seas?|rivers?|streams?|rains?|storms?|thunder|winds?|'
    r'forest|jungle|meadow|harbours?|harbors?|beach|shores?|surfs?|gulls?|birds?|'
    r'birdsong|whales?|crickets?|frogs?|wolves?|dolphins?|owls?|sparrows?|'
    r'raindrops?|droplets?|canopy|mist|fog|dew|breeze|tides?|glade|brook)\b',
]), re.IGNORECASE)

# Wellness-factory artist names — additional patterns for stock acts
# that slip through the density check because they use "real-sounding" words
_WELLNESS_ARTISTS_EXTRA = re.compile(r'|'.join([
    # "X Guru/Master/Expert" — stock wellness branding
    r'\b(harmony|plant|sleep|meditation|yoga|zen|chakra|healing|wellness|mindful)\s+(guru|master|expert|academy|institute|studio)\b',
    # "Sleep is X" / "X is Life" patterns
    r'\bsleep\s+is\b',
    r'\b(music|sound|noise|rain|ocean|nature)\s+is\s+(life|love|peace|bliss|healing)\b',
    # Artist name is literally a wellness claim
    r'^(sleep|relax|calm|heal|meditat)\w*\s+(is|for|with)\s+',
]), re.IGNORECASE)

# Wellness-factory and genre-descriptor artist names — reject regardless of track title
_WELLNESS_ARTISTS = re.compile(r'|'.join([
    # Known wellness factories (original)
    r'soothing ambient', r'miracle tones', r'sacred society',
    r'dream supplier', r'ocean therapy', r'nature sound retreat',
    r'meditation spa', r'deep sleep music', r'erhu sleep',
    r'tranquility spa', r'oriental meditation music', r'wp sounds',
    r'big secret music', r'relaxation music', r'healing music',
    r'sleep music collective', r'frqncy', r'musicoterapia',
    r'sound bath.*music', r'music.*sound bath',
    r'432 hz', r'528 hz', r'639 hz', r'741 hz',
    # Lofi / chillout factory brands
    r'chill out dreamscapes', r'chillout deep',
    r'lo[- ]?fi gigi', r'lofi waiter', r'lofi fruits', r'chill fruits',
    r'\blofi beats\b', r'lo[- ]?fi chillout', r'lo[- ]?fi hip.?hop radio',
    r'chill hop music', r'lofi hip.?hop music',
    # Background / ambient factories
    r'background music experience', r'\bpure massage music\b',
    r'\bambient pads\b', r'peaceful pieces\b',
    # Relaxation / sleep factories
    r'sleep underwriter', r'\brelaxed minds\b',
    r'relaxing music retreat', r'relaxing music for\b',
    r'relaxing restaurant music', r'sleepy meditation',
    # Instrument-as-artist (standalone instrument name used as act name)
    # e.g. "guqin", "LOFI CHILE, guqin" — caught via primary artist check below
    r'^guqin$', r'^erhu$', r'^gamelan$', r'^sitar$', r'^tabla$',
    r'^koto$', r'^shamisen$', r'^pipa$', r'^shakuhachi$',
    # Multi-genre compound artist names: artist field lists genre keywords
    # e.g. "Calm Music, Meditation Music, Relaxing Music, ..."
    r'(calm|relaxing|meditation|ambient|healing|spa|study|lofi|chill)\s+music'
    r'.{1,30}(calm|relaxing|meditation|ambient|healing|spa|study|lofi|chill)\s+music',
    # Christian background music factories
    r'christian instrumental (guitar|piano|worship|music)',
    # "[Culture] Traditional [anything]" — descriptor as artist name, not a real act
    # e.g. "Chinese Traditional", "Chinese Traditional Erhu Music",
    #      "Chinese Traditional Muses", "Indian Traditional Ensemble"
    r'(chinese|japanese|korean|indian|balinese|tibetan|arabic|persian|celtic|'
    r'african|thai|vietnamese|mongolian|turkish|oriental|asian|eastern|western)\s+'
    r'traditional\b',
    # "Traditional [Culture] [anything]" — reversed word order
    # e.g. "Traditional Chinese Music", "Traditional Indian Ensemble"
    r'\btraditional\s+(chinese|japanese|korean|indian|balinese|tibetan|arabic|'
    r'persian|celtic|african|thai|vietnamese|mongolian|turkish|oriental|asian|eastern)\b',
    # "[Culture] [single music-descriptor word]" as full artist name
    # e.g. "Chinese Traditional Music Ensemble", "Chinese Relaxation and Meditation",
    #      "Chinese Channel", "Chinese traditional music"
    r'^(chinese|japanese|korean|indian|balinese|tibetan|arabic|persian|oriental|'
    r'asian|eastern)\s+(music|muses|channel|sounds?|ensemble|harmony|meditation|'
    r'relaxation|healing)\b',
    # "[Adj/Various] [Culture] musician(s)" — any casing
    # e.g. "BALINESE MUSICIAN", "Various Balinese musicians"
    r'(balinese|chinese|japanese|korean|indian|tibetan|thai|vietnamese|mongolian)\s+'
    r'musicians?\b',
    # Lullaby/nursery/kids factory artists
    r'\bbaby\s+(lullaby|lullabies|music|sleep|sounds?)\b',
    r'\b(lullaby|lullabies)\s+(academy|collective|music|club|renditions|players?)\b',
    r'\bnursery\s+rhymes?\b',
    r'\bkids?\s+(music|songs?|bop|dance)\b',
    r'\bcoccole\s+sonore\b',
    r'\btiny\s+boppers?\b',
    r'\bchildren.?s?\s+(sing|song|music|choir)\b',
    # Stock/catalogue labels used as artist names
    r'^world music$',
    r'heart of the dragon',
    r'\bchinese channel\b',
    # Bare genre-name-as-artist (whole field is the genre word)
    r'^(drum\s*[&n]?\s*bass|dnb|d\s*&\s*b|darkstep|drumstep|liquid\s+dnb|psytrance|gabber|hardcore)$',
    r'^(trance|techno|house|edm|dubstep|garage\s+music|grime|trap\s+music|tech\s+house|deep\s+house)$',
    r'^(k-?pop|j-?pop|c-?pop|hip-?hop|r&b|reggaeton|afrobeats?|amapiano|cumbia|bachata|salsa|merengue|dancehall|reggae|ska|punk|metal|jazz|blues|soul|funk|disco|grunge|emo)$',
    # Artist names that are "[letters]beats/music/sound" compounds
    r'^[a-z]{2,10}(beats|music|sounds?|tones?|tunes?|mixes|studio)$',
    # "Genre + Zone/Project/Club/Collective/Station" — stock branding. NOTE: no
    # "company", "ensemble", or "tribe" — those show up in legit band names
    # (e.g. Blues Company, Piano Ensemble, A Tribe Called Quest).
    r'^(bossa\s+nova|jazz|lofi|lo-fi|chill|ambient|classical|reggae|blues|soul|funk)\s+'
    r'(zone|project|club|collective|cafe|café|corner|channel|group|sessions?|vibes?|'
    r'factory|station|radio)\b',
    # "[Genre] International/Worldwide/Global" — tight genre-prefix + globalism
    # suffix. Real African/Thai/Balkan acts named "... International Band" are
    # anchored on a proper noun (Oriental Brothers, Paradise Bangkok Molam,
    # Ubulu, Peacocks Guitar Band…) — none start with a bare genre descriptor.
    r'^(ambient|chill|chillout|chill\s+out|chillwave|chillstep|lofi|lo-fi|'
    r'new\s+age|deep\s+house|meditation|relax|wellness|sleep|focus|study|'
    r'yoga|zen|spa)\s+'
    r'(international|worldwide|global|recordings|records)\b',
    # "[Genre] [Word] Station/Radio/Sessions/Lounge" — 3-token stock brand.
    # Suffix is deliberately NOT "project" (Funk Shui Project is real) or
    # "collective" (many real acts).
    r'\b(jazz|blues|rock|pop|soul|funk|lofi|lo-fi|chill|ambient|classical|reggae|'
    r'indie|electronic|acoustic)\s+\w+\s+'
    r'(station|radio|sessions?|lounge)\b',
    # "[Instrument] + Chill/Zone/Vibes" — stock mood branding. NOT "ensemble",
    # "collective", "project", "club" — those are common in legit group names
    # (Piano Ensemble, Drum Collective, Sax Project…). Only the inarguably
    # mood-branded suffixes.
    r'\b(guitar|piano|violin|flute|strings?|cello|saxophone|sax|harp|drums?)\s+'
    r'(chill|chillout|chillwave|vibes?|zone|lounge|mood|moods|flow)\b',
    # "New Age / Spiritual / Tribal / Shamanic / Mystical / Sacred [X]" — wellness artist
    # e.g. "New Age Spiritual Musician", "Shamanic Healing Drums", "Cosmic Meditation Tribe"
    r'\b(new\s+age|spiritual|tribal|shamanic|shaman|mystical|celestial|cosmic|'
    r'ancient|sacred|ethereal|astral|angelic)\s+'
    r'(spiritual\s+|healing\s+|meditation\s+|ambient\s+)?'
    r'(musician|artist|healer|guide|channel|ensemble|traveler|journey|drummers?|'
    r'drums?|sounds?|tones?|tribe|collective)\b',
    # "[Genre] Fusion/Mosaic/Tapestry" 3-token compound-as-artist — factory branding
    # e.g. "Folk Mosaic Fusion", "World Music Tapestry" (must be 3 tokens; 2-token
    # forms like "Folk Mosaic" could be real acts)
    r'^(folk|jazz|world|cultural|ethnic|global|classical|ambient|chill)\s+'
    r'(mosaic|fusion|tapestry|journey|odyssey|voyage|collective|ensemble|project)'
    r'\s+(fusion|mosaic|tapestry|ensemble|collective|project|sounds?|music)\s*$',
    # "DJ Relax/Chill/Sleep" + "BGM/Music" — stock background music acts
    r'\bdj\s+(relax|chill|sleep|calm|zen|ambient|meditation)\b',
    r'\b(relax|chill|sleep|calm)\s+bgm\b',
    # "Sound Effects" in artist name
    r'\bsound\s+effects?\b',
    # "Noise Machine/Generator" in artist name
    r'\b(noise|sound)\s+(machine|generator)\b',
    r'^(study|focus|sleep|relax|calm|chill|ambient|lofi|lo-?fi|chillhop|lounge)\s+(beats?|music|vibes?|sounds?)$',
    # Instrument-name + "Music" / "Sounds" as the entire artist name
    # e.g. "Sitar Music", "Erhu Sounds", "Guqin Music", "Gamelan Sounds"
    r'^(erhu|guqin|guzheng|pipa|qin|koto|sitar|tabla|shamisen|gamelan|shakuhachi|'
    r'kalimba|hang|didgeridoo|kora|oud|saz|hulusi|dizi|haegeum)\s+(music|sounds?|tones?)$',
    # "X Lab / Studio / Factory / Collective / Works" — SEO/stock branding
    # e.g. "Official Ambience Lab", "Drill Music Studio", "Ambient Music Factory"
    r'\b(ambience|ambient|music|sound|sounds|beat|beats|loop|loops|noise|drone|vocal|choir)\s+'
    r'(lab|labs|studio|studios|workshop|collective|academy|works|forge|foundry)\b',
    # Christian / worship factory acts
    r'\bpraise\s+(and|&)\s+worship\b',
    r'\binstrumental\s+christian\b',
    r'\bchristian\s+(instrumental|piano|guitar|worship)\s+(music|songs?|hymns?)?\b',
    # Generic "[adj] Mix / Collection / Radio / Sessions" — compilations branded as acts
    # e.g. "Chillstep Ambient Mix", "Lofi Study Mix"
    r'\b(ambient|chillout|chill|chillwave|chillstep|lofi|lo-fi|study|focus|'
    r'sleep|relaxing|deep|spa|zen|meditation|workout)\s+'
    r'(mix|mixes|collection|radio|sessions?|compilation|playlist)\b',
    # Wellness crystal/bowl branding
    r'\bquartz\s+crystal\b',
    r'\b(crystal|singing)\s+bowls?\s+(energy|music|healing|sounds?|vibes?|meditation)?\b',
    # German white-noise / sleep-music factory acts (artist names)
    r'\bwei[ßs]+e[rns]?\s+(rauschen|l[aä]rm)\b',
    r'\b(meeres|wind|regen|ozean|natur|wald)rauschen\b',
    r'\bbabyschlaf\b',
    # "Kids Tunes Club" / "Kids Songs Club" / "Kids Music" factory acts
    r'\bkids?\s+(tunes?|songs?|music|beats?|nursery)\s+(club|collective|band|'
    r'academy|studio|group|channel|radio)\b',
    # "Learn with X" / "ABC Alphabet" / "Sing Along with" — educational-kids stock
    r'\blearn\s+with\s+\w+\b',
    r'\babc\s+(alphabet|song|learning)\b',
    # CJK stock-music-factory artist names: wellness/sleep/baby/yoga descriptor
    # paired with 音樂/音楽 (music), 旋律 (melody), 樂章/乐章, or 節奏/节奏.
    # e.g. "微笑旋律音樂" (Smile Melody Music), "睡眠寶寶貴族音樂" (Sleep Baby
    # Noble Music), "鋼琴放鬆輕聽 古典音樂". Real CJK artists (鄧麗君, 周杰倫,
    # YOASOBI, 宇多田ヒカル, 五月天) don't contain any of these wellness words.
    r'(微笑|放鬆|放松|寶寶|宝宝|瑜伽|冥想|助眠|安眠|睡眠|輕柔|轻柔|'
    r'治癒|治愈|療癒|疗愈|催眠|舒緩|舒缓|紓壓|纾压|胎音|靜心|静心|'
    r'深度|輕音|轻音|貴族|贵族|養生|养生|悠然|純音|纯音|疲勞|疲劳|'
    r'癒し|ヒーリング|リラックス|リラクゼーション|'
    r'子守唄|寝かしつけ)'
    r'[^a-zA-Z0-9]{0,15}'
    r'(音[樂楽律]|旋律|節奏|节奏|樂章|乐章|音色|古箏|古筝|古琴|二胡|琵琶)',
    # Bare stock-compound CJK words that slip past the distance-based match
    # because wellness and music characters overlap (e.g. 輕音樂 = 輕音+樂 but
    # the 音 is shared). These phrases are unambiguous stock-compilation
    # signatures.
    r'輕音樂|轻音乐|養生音樂|养生音乐|療愈音律|疗愈音律|助眠音樂|助眠音乐|'
    r'純音樂|纯音乐|放鬆音樂|放松音乐',
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


_REPEATED_DESCRIPTORS = {
    'bossa', 'jazz', 'blues', 'reggae', 'samba', 'flamenco', 'tango', 'funk',
    'soul', 'lofi', 'lo-fi', 'ambient', 'chillout', 'chill', 'chillwave',
    'chillstep', 'techno', 'trance', 'house', 'dnb', 'country', 'folk',
    'christian', 'worship', 'praise', 'gospel', 'hymns', 'meditation', 'spa',
    'study', 'focus', 'sleep', 'yoga', 'zen', 'healing', 'relaxing', 'wellness',
    'lounge', 'cocktail', 'dinner', 'background', 'cafe',
    'piano', 'guitar', 'violin', 'flute', 'strings', 'cello',
    'instrumental', 'acoustic', 'orchestral', 'electronic', 'classical',
}


def _has_repeated_descriptor(artist_name):
    """True if a descriptor word (bossa, christian, lofi, …) appears in 2+
    comma-separated artist parts — strong sign the "artist" is really a
    comma-glued bundle of stock labels, e.g.
      "Bossa Lounge, Sunset Bossa, Brazilian Bossa"
      "Instrumental Christian Songs, Christian Piano Music, Praise and Worship"
    """
    if not artist_name or "," not in artist_name:
        return False
    parts = [p.strip().lower() for p in artist_name.split(",") if p.strip()]
    if len(parts) < 2:
        return False
    counts = {}
    for p in parts:
        tokens = set(re.split(r"[\s\-_&/]+", p))
        for w in _REPEATED_DESCRIPTORS:
            if w in tokens:
                counts[w] = counts.get(w, 0) + 1
    return any(v >= 2 for v in counts.values())


_INSTITUTIONAL = re.compile(
    r'\b(university|college|school|academy|institute|conservatory|ensemble|'
    r'orchestra|choir|symphony|philharmonic|quartet|trio|sextet|chorale|'
    r'big band|collective society)\b',
    re.IGNORECASE,
)

# Patterns where a real institution (University X Music Studio, Chamber
# Orchestra, etc.) would be a false positive. Only applied to the artist-side
# stock-brand check, not to true trash patterns.
_INSTITUTIONAL_SAFE_WELLNESS = re.compile(
    r'\b(ambience|ambient|music|sound|sounds|beat|beats|loop|loops|noise|drone|vocal|choir)\s+'
    r'(lab|labs|studio|studios|workshop|collective|academy|works|forge|foundry)\b',
    re.IGNORECASE,
)


def _load_artist_blacklist():
    """Load the per-artist blacklist from the artist_blacklist table once
    per process. Returns frozenset of normalised artist keys (lowercased,
    primary artist = first comma-segment). Empty set if the table doesn't
    exist or DB is unreachable — fail-open by design, the regex rules still
    apply.
    """
    try:
        from lib.db import fetchall as _fetch
        rows = _fetch("SELECT artist_key FROM artist_blacklist")
        return frozenset(r["artist_key"] for r in rows if r.get("artist_key"))
    except Exception:
        return frozenset()


# Module-level cache. Cron processes reload on each invocation (fresh
# import); long-running server processes get a stale view until restart,
# which is acceptable for an additive blacklist (false negatives, never
# false positives, until the container is restarted).
_ARTIST_BLACKLIST = _load_artist_blacklist()


def _is_blacklisted(artist_name):
    """True if the primary artist (first comma-segment, lowercased) is in
    the blacklist."""
    if not artist_name or not _ARTIST_BLACKLIST:
        return False
    primary = (artist_name.split(",")[0] or "").strip().lower()
    return bool(primary and primary in _ARTIST_BLACKLIST)


def is_trash(track_name, artist_name="", album_name=""):
    """Return True if the track name, artist, or album looks like non-music content."""
    # Per-artist blacklist (one-off stock acts that don't fit any pattern)
    if artist_name and _is_blacklisted(artist_name):
        return True
    if _ALWAYS_TRASH.search(track_name):
        return True
    if _REGIONAL_TRASH.search(track_name):
        return True
    if _MULTILINGUAL_RELAX.search(track_name):
        return True
    if artist_name and _WELLNESS_ARTISTS.search(artist_name):
        # Academic/institutional artists that happen to contain "music lab"
        # or "electronic music studio" are NOT trash — exempt them if the
        # match is ONLY the lab/studio pattern.
        if _INSTITUTIONAL.search(artist_name) and _INSTITUTIONAL_SAFE_WELLNESS.search(artist_name):
            # Try again without the institutional-safe pattern
            stripped = _INSTITUTIONAL_SAFE_WELLNESS.sub('', artist_name)
            if not _WELLNESS_ARTISTS.search(stripped):
                pass  # exempt — continue to other checks
            else:
                return True
        else:
            return True
    if artist_name and _WELLNESS_ARTISTS_EXTRA.search(artist_name):
        return True
    if artist_name and _MULTILINGUAL_RELAX.search(artist_name):
        return True
    # Also check the title-junk patterns against artist name — stock acts
    # often put the functional descriptor in their name ("Music for Reading")
    if artist_name and _ALWAYS_TRASH.search(artist_name):
        return True
    # Comma-glued stock-label artist bundles (e.g. "Bossa Lounge, Sunset Bossa, …")
    if artist_name and _has_repeated_descriptor(artist_name):
        return True
    # Check each individual artist in a collaboration listing
    if artist_name and "," in artist_name:
        for part in artist_name.split(","):
            part = part.strip()
            if part and _WELLNESS_ARTISTS.search(part):
                return True
            if part and _MULTILINGUAL_RELAX.search(part):
                return True
            if part and _is_stock_word_pile(part):
                return True
    # Stock-word density check: artist or title made of 2+ stock-music descriptor words
    if artist_name and _is_stock_word_pile(artist_name):
        return True
    if _is_stock_word_pile(track_name, threshold=3):
        return True
    # Album-name junk detection: compilation/stock album names
    if album_name and _is_junk_album(album_name):
        return True
    # Long SEO-style titles (10+ words with romantic/journey/ambient keywords)
    if _is_seo_title(track_name):
        return True
    return False


# ── Junk album detection ──────────────────────────────────────────────────────
_JUNK_ALBUM = re.compile(r'|'.join([
    r'\bfinest\s+(dinner|lounge|cocktail|spa|relaxing|chill)\s+music\b',
    r'\b(dinner|lounge|cocktail|spa|massage|yoga|meditation)\s+music\b',
    r'\bmusic\s+for\s+(dinner|lounge|cocktail|spa|massage|yoga|meditation|study|sleep|focus|work)\b',
    r'\brelax(ing|ation)?\s+(music|sounds?|vibes?|tunes?)\b',
    r'\b(sleep|study|focus|work|chill)\s+(music|sounds?|vibes?|tunes?|beats?)\b',
    r'\b(background|elevator|waiting\s+room|lobby)\s+music\b',
    r'\bvocal\s+exercise',
    r'\bvocal\s+warmup',
    r'\bmusic\s+therapy\b',
    # Kids/lullaby/nursery compilations
    r'\bbaby\s+(lullab|sleep|music|sooth)',
    r'\b(lullab|nursery).*(sooth|calm|sleep|baby|toddler|infant)',
    r'\bchildren.?s?\s+sing\s+along\b',
    r'\bkids?\s+(song|music|favorite|classic)',
    r'\bninna\s+nanna\b',
    r'\bfacciamo\s+la\s+nanna\b',
    # Stand-up comedy albums (not music)
    r'\bcomedy\s+(club|special|album|show|hour|set|central|routine|tour)\b',
    r'\b(stand[- ]?up|spoken\s+word)\s+(comedy|special|album|routine|set)\b',
    r'\blive\s+(at|from)\s+(the\s+)?(apollo|improv|laugh\s+factory|caroline\'?s|'
    r'comedy\s+(cellar|store|club))\b',
    # "Mongolian Throat Singing for Peace" / wellness throat-singing compilations
    r'\bthroat\s+(singing|chanting)\b.{0,40}\b(peace|healing|meditation|relaxation|'
    r'harmony|calm|serenity|wellness|sleep|bliss)\b',
    # "Relaxing [Culture] Ambient" album compilations
    r'\brelax(ing|ation)?\s+(chinese|japanese|korean|indian|balinese|tibetan|'
    r'arabic|persian|celtic|african|thai|vietnamese|mongolian|turkish|oriental|'
    r'asian|eastern|nordic|andalus(ian)?|spanish|flamenco|mediterranean|tropical|'
    r'latin|brazilian)\s+(ambient|instrumental|music|sounds?|tones?|meditation)\b',
    # "Relaxing Forest/Rain/Ocean" compilation albums
    r'\brelax(ing|ation)?\s+(forest|ocean|rain|nature|birds?|meadow|river|stream|'
    r'jungle|beach|waves?)\s+(rain|sounds?|music|ambient|meditation)?\b',
    # "New Age Tones" / "Deep Sleep ..." compilation albums
    r'\bnew\s+age\s+(tones?|sounds?|music|meditation|vibes?|mix)\b',
    # German wellness album terms
    r'\bbabyschlaf\b',
    r'\bwei[ßs]+e[rns]?\s+(rauschen|l[aä]rm)\b',  # "Weißes Rauschen", "Weißer Lärm"
    r'\b(meeres|wind|regen|ozean|natur|wald)rauschen\b',
    # SEO-title album patterns with stacked genre descriptors
    # e.g. "Whispers of Electronic Indie Nights", "Echoes of Ambient Dreams"
    r'\b(whispers?|echoes?|shadows?|sounds?|tales?|visions?|dreams?)\s+of\s+'
    r'(ambient|atmospheric|electronic|indie|acoustic|instrumental|chill|cinematic)\s+',
    # Numbered-decade factory compilation albums
    # e.g. "Synthpop 1980s Tokyo Girl", "Chillwave 2010s Beach Party"
    r'^(synthpop|synthwave|chillwave|vaporwave|ambient|chillout|chill\s+out|'
    r'lofi|lo-fi)\s+\d{4}s?\s+',
]), re.IGNORECASE)


def _is_junk_album(album_name):
    if not album_name:
        return False
    return bool(_JUNK_ALBUM.search(album_name))


# ── Long SEO-title detection ─────────────────────────────────────────────────
_SEO_KEYWORDS = {
    'journey', 'whisper', 'whispers', 'whispering', 'romance', 'romantic',
    'passionate', 'passion', 'soulful', 'melodies', 'melody', 'sensual',
    'serenade', 'serenity', 'tranquil', 'tranquility', 'blissful',
    'enchanting', 'mesmerizing', 'captivating', 'breathtaking',
    'ambient', 'ambience', 'ambiance', 'atmospheric',
}


def _is_seo_title(title):
    """Detect factory SEO titles like 'Andalusian Whisper: A Journey Through
    Passionate Flamenco Guitar and Soulful Romantic Melodies'"""
    if not title:
        return False
    words = title.lower().split()
    if len(words) < 8:
        return False
    seo_hits = sum(1 for w in words if w.strip('.:,;![]()') in _SEO_KEYWORDS)
    # If 3+ SEO keywords in a 8+ word title, it's stock content
    if seo_hits >= 3:
        return True
    # "X: A Journey Through Y" pattern
    if re.search(r':\s*a\s+journey\s+(through|into|of)\b', title, re.IGNORECASE):
        return True
    # Bracket-enclosed SEO spam: [Loopable Loop No Fade Baby Sleep...]
    if re.search(r'\[.*\b(loopable|loop|no\s+fade|sound\s+effect|background|ambien[ct]e?)\b.*\]', title, re.IGNORECASE):
        return True
    # Parenthetical functional descriptors: (Version 2) [Baby Sleep Relaxing...]
    if re.search(r'\b(sound\s+effect|background\s+noise|white\s+noise|ambien[ct]e?\s+sound)\b', title, re.IGNORECASE):
        return True
    return False


# ── Multilingual "for sleep / for study / for relax" patterns ─────────────────
# Catches stock content in non-English: "Som de Chuva Para Dormir", "Música para
# dormir", "Bruit pour dormir", "Musik zum Schlafen", "Musica per dormire"
_MULTILINGUAL_RELAX = re.compile(r'|'.join([
    r'\bpara\s+(dormir|estudiar|relajarse|meditar|trabajar|leer|concentrar)\b',  # Spanish/Portuguese
    r'\bsom\s+(de|do|da|para)\s+(chuva|sono|relax|[uú]tero|natureza|mar|floresta|chuva)', # Portuguese
    r'\bbeb[eê]\s+(dormindo|relaxando|acalmando)',                               # Portuguese baby
    r'\bsom\s+do\s+[uú]tero\b',                                                 # womb sound
    r'\bmusica\s+(para|de)\s+(dormir|estudiar|relax|meditaci)',                  # Spanish/Italian
    r'\bm[uú]sica\s+(para|de)\s+(dormir|estudiar|relax|meditaci)',
    r'\bpour\s+(dormir|[ée]tudier|se\s+relaxer|m[ée]diter|se\s+concentrer)\b',   # French
    r'\bzum\s+(schlafen|lernen|entspannen|meditieren|arbeiten|lesen)\b',         # German
    r'\bper\s+(dormire|studiare|rilassarsi|meditare|leggere)\b',                 # Italian
    r'\bdla\s+(snu|relaks|nauki|medytacji)\b',                                   # Polish
    r'\bnoiz\b|\bbrown\s+noise\b|\bpink\s+noise\b',                              # noise variants
    # Multilingual "sleep well" / "good sleep" factory-artist markers
    r'\bbien\s+dorm(ir)?\b',                                                      # Spanish/French "sleep well"
    r'\bduerm[aeo]?s?\s+bien\b',                                                  # Spanish "sleep well"
    r'\bdormi\s+bene\b',                                                          # Italian "sleep well"
    r'\bschlaf\s+gut\b',                                                          # German "sleep well"
    r'\bbon\s+sommeil\b',                                                         # French "good sleep"
]), re.IGNORECASE)


# ── Stock-word density: artist/title built from genre/mood descriptors ────────
# Words that, when piled up in an artist or title without a real proper noun,
# strongly suggest factory/stock content. NOT individually disqualifying — only
# when 2+ are stacked AND no clear proper-noun word breaks the pattern.
_STOCK_WORDS = {
    'ambient', 'atmospheric', 'cinematic', 'epic', 'orchestral', 'instrumental',
    'electronic', 'electric', 'electronica', 'electro', 'techno', 'trance', 'house', 'edm',
    'indie', 'pulse', 'melody', 'melodic', 'acoustic',
    'dnb', 'darkstep', 'drumstep', 'liquid', 'psytrance', 'breaks', 'breakbeat',
    'lofi', 'lo-fi', 'chill', 'chillout', 'chillhop', 'hiphop', 'beats', 'beat',
    'relax', 'relaxing', 'relaxation', 'calm', 'calming', 'soothing', 'tranquil',
    'peaceful', 'serene', 'serenity', 'bliss', 'blissful', 'gentle', 'soft',
    'sleep', 'sleeping', 'sleepy', 'dream', 'dreamy', 'dreams', 'dreamscape',
    'study', 'studying', 'focus', 'focused', 'concentration', 'productivity',
    'work', 'working', 'meditation', 'meditative', 'mindful', 'mindfulness',
    'healing', 'wellness', 'spa', 'zen', 'reiki', 'yoga', 'therapy', 'therapeutic',
    'pure', 'deep', 'cosmic', 'celestial', 'ethereal', 'astral', 'galactic',
    'sounds', 'sound', 'tones', 'tone', 'waves', 'wave', 'frequencies', 'frequency',
    'music', 'tunes', 'sonic', 'vibes', 'vibe', 'mood', 'moods', 'mode',
    'background', 'ambience', 'soundscape', 'soundscapes', 'pads', 'textures',
    'cleanmind', 'cleanmindsounds', 'mindfulsounds', 'puresounds', 'softsounds',
    'study-focus', 'sleepwell', 'workflow', 'flowstate',
    'breathe', 'mindful',
    'motivational', 'motivation', 'inspirational',
    'technology', 'cybernetic', 'futuristic',
    # Re-added (were excluded but needed for stock-artist detection):
    'lounge', 'ocean', 'rain', 'morning', 'evening', 'night',
    'dinner', 'cocktail', 'cafe', 'coffee',
    'trip', 'journey', 'whisper', 'whispers', 'romance', 'romantic',
    'passionate', 'soulful', 'sensual', 'sexy',
    'exercise', 'vocal', 'drill', 'studio',
    'mental', 'finest', 'echos', 'echoes',
    'ambience', 'ambiance',
    # German/multilingual wellness
    'binaurale', 'binaural', 'tiefschlaf', 'entspannung', 'schlaf',
    'erfahrung', 'frequenz',
    # Wellness-branding words
    'lazy', 'cozy', 'harmony', 'guru', 'tribe', 'path', 'garden',
    'oasis', 'haven', 'sanctuary', 'retreat', 'nest',
    # Note: 'soul', 'guitar', 'piano', 'flute', 'mind', 'spirit', 'energy'
    # still excluded — real acts use them ("Ocote Soul Sounds" etc.)
}

# Words that strongly suggest a real act/song (proper-noun-ish or specific terms).
# If any of these appear, we don't flag based on stock-word density alone.
_NON_STOCK_HINTS = re.compile(
    r"\b(?:feat\.?|featuring|presents?|"
    r"records?|orchestra|quartet|quintet|trio|sextet|"
    r"DJ|MC|Sir|Lord|Lady|King|Queen|Prince|Saint|"
    r"Mr|Mrs|Ms|Dr|Prof|the|of|and|von|de|la|el|los|las|du|der|die|das"
    r")\b",
    re.IGNORECASE,
)


def _is_stock_word_pile(text, threshold=2):
    """True if the text reads like a pile of stock-music descriptor words.

    threshold=2 (default, for artist names): "Atmospheric Techno", "Relax a Wave"
    threshold=3 (for titles): "Study Focus Trance", "Ambient Technology Electronic"
    """
    if not text:
        return False
    s = text.strip().lower()
    # Tokenize on whitespace + common separators (- _ & , .)
    tokens = re.split(r"[\s\-_&,.()[\]{}:;/]+", s)
    tokens = [t for t in tokens if t]
    if not tokens:
        return False
    # Case 1: a single smashed compound like "cleanmindsounds", "studymusic",
    # "ambientchill" — match against stock-word fragments
    if len(tokens) == 1 and len(tokens[0]) >= 8:
        token = tokens[0]
        hits = sum(1 for w in ('clean', 'mind', 'sound', 'music', 'sleep', 'study',
                               'focus', 'chill', 'relax', 'calm', 'peace', 'soft',
                               'pure', 'deep', 'zen', 'spa', 'wellness', 'ambient',
                               'tranquil', 'serene', 'soothing', 'lofi', 'beats',
                               'beat', 'vibes', 'tones', 'waves')
                   if w in token)
        if hits >= 2:
            return True
    # Case 2: multi-token pile — count stock-word tokens
    stock_hits = sum(1 for t in tokens if t in _STOCK_WORDS)
    # Allow if there's a clear non-stock anchor (proper noun, "the", "of", etc.)
    if stock_hits >= threshold:
        # If most tokens are stock and there's no clear non-stock anchor → trash
        non_stock = len(tokens) - stock_hits
        # Count "real" non-stock tokens (excluding articles/prepositions)
        articles = {'a', 'an', 'the', 'of', 'and', '&', 'in', 'on', 'for', 'to', 'with'}
        real_non_stock = sum(1 for t in tokens if t not in _STOCK_WORDS and t not in articles)
        if real_non_stock <= 1 and not _NON_STOCK_HINTS.search(text):
            return True
    return False
