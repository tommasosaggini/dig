"""One canonical spelling per place — the region axis's normalize_genres.

The pool's region axis reached 442 distinct values for ~200 real places
(audited 2026-08-11): 'USA'/'US'/'United States' as three rows, bare ISO-2
codes ('UA', 'NG') from the MB drain's country field, cities ('Tbilisi',
'Lagos', 'Kyïv') from MB areas and Bandcamp locations, provinces ('Ontario',
'Québec') from Bandcamp's last-comma-token fallthrough. Every consumer that
groups by region — the picker's coverage axis, the working-set sampler, the
world-coverage view — silently fragmented across the variants.

`canonical_region(value)` is the single answer:
  * ISO-2 codes → the country's English name;
  * known synonyms / cities / subdivisions → their country;
  * macro-regions the taxonomy uses on purpose (West Africa, Nordic,
    Caribbean, …) pass through untouched;
  * anything unrecognized passes through trimmed — this module maps what it
    KNOWS and never guesses, so a new value drifts visibly rather than
    being mangled.

Lookups are case-insensitive and diacritic-folded ('Québec', 'Bakı'), so a
spelling variant of a known alias still lands. Kept deliberately: historic
entities ('Soviet Union'), real small territories (Greenland, Martinique,
Faroe Islands) — a place being tiny is not drift.

Write-time users: ingest_mb_artists, bandcamp.location_to_country, curator
ingest. Backfill: scripts/normalize_pool.py.
"""

import unicodedata

# ── ISO 3166-1 alpha-2 → English short name ──────────────────────────────────
ISO2 = {
    "AD": "Andorra", "AE": "United Arab Emirates", "AF": "Afghanistan",
    "AG": "Antigua and Barbuda", "AI": "Anguilla", "AL": "Albania",
    "AM": "Armenia", "AO": "Angola", "AR": "Argentina", "AT": "Austria",
    "AU": "Australia", "AW": "Aruba", "AZ": "Azerbaijan",
    "BA": "Bosnia and Herzegovina", "BB": "Barbados", "BD": "Bangladesh",
    "BE": "Belgium", "BF": "Burkina Faso", "BG": "Bulgaria", "BH": "Bahrain",
    "BI": "Burundi", "BJ": "Benin", "BM": "Bermuda", "BN": "Brunei",
    "BO": "Bolivia", "BR": "Brazil", "BS": "Bahamas", "BT": "Bhutan",
    "BW": "Botswana", "BY": "Belarus", "BZ": "Belize", "CA": "Canada",
    "CD": "DR Congo", "CF": "Central African Republic", "CG": "Congo",
    "CH": "Switzerland", "CI": "Ivory Coast", "CK": "Cook Islands",
    "CL": "Chile", "CM": "Cameroon", "CN": "China", "CO": "Colombia",
    "CR": "Costa Rica", "CU": "Cuba", "CV": "Cape Verde", "CW": "Curaçao",
    "CY": "Cyprus", "CZ": "Czechia", "DE": "Germany", "DJ": "Djibouti",
    "DK": "Denmark", "DM": "Dominica", "DO": "Dominican Republic",
    "DZ": "Algeria", "EC": "Ecuador", "EE": "Estonia", "EG": "Egypt",
    "ER": "Eritrea", "ES": "Spain", "ET": "Ethiopia", "FI": "Finland",
    "FJ": "Fiji", "FM": "Micronesia", "FO": "Faroe Islands", "FR": "France",
    "GA": "Gabon", "GB": "United Kingdom", "GD": "Grenada", "GE": "Georgia",
    "GF": "French Guiana", "GH": "Ghana", "GI": "Gibraltar",
    "GL": "Greenland", "GM": "Gambia", "GN": "Guinea", "GP": "Guadeloupe",
    "GQ": "Equatorial Guinea", "GR": "Greece", "GT": "Guatemala",
    "GU": "Guam", "GW": "Guinea-Bissau", "GY": "Guyana", "HK": "Hong Kong",
    "HN": "Honduras", "HR": "Croatia", "HT": "Haiti", "HU": "Hungary",
    "ID": "Indonesia", "IE": "Ireland", "IL": "Israel", "IM": "Isle of Man",
    "IN": "India", "IQ": "Iraq", "IR": "Iran", "IS": "Iceland",
    "IT": "Italy", "JE": "Jersey", "JM": "Jamaica", "JO": "Jordan",
    "JP": "Japan", "KE": "Kenya", "KG": "Kyrgyzstan", "KH": "Cambodia",
    "KI": "Kiribati", "KM": "Comoros", "KN": "Saint Kitts and Nevis",
    "KP": "North Korea", "KR": "South Korea", "KW": "Kuwait",
    "KY": "Cayman Islands", "KZ": "Kazakhstan", "LA": "Laos",
    "LB": "Lebanon", "LC": "Saint Lucia", "LI": "Liechtenstein",
    "LK": "Sri Lanka", "LR": "Liberia", "LS": "Lesotho", "LT": "Lithuania",
    "LU": "Luxembourg", "LV": "Latvia", "LY": "Libya", "MA": "Morocco",
    "MC": "Monaco", "MD": "Moldova", "ME": "Montenegro",
    "MG": "Madagascar", "MH": "Marshall Islands", "MK": "North Macedonia",
    "ML": "Mali", "MM": "Myanmar", "MN": "Mongolia", "MO": "Macao",
    "MQ": "Martinique", "MR": "Mauritania", "MT": "Malta",
    "MU": "Mauritius", "MV": "Maldives", "MW": "Malawi", "MX": "Mexico",
    "MY": "Malaysia", "MZ": "Mozambique", "NA": "Namibia",
    "NC": "New Caledonia", "NE": "Niger", "NG": "Nigeria",
    "NI": "Nicaragua", "NL": "Netherlands", "NO": "Norway", "NP": "Nepal",
    "NR": "Nauru", "NU": "Niue", "NZ": "New Zealand", "OM": "Oman",
    "PA": "Panama", "PE": "Peru", "PF": "French Polynesia",
    "PG": "Papua New Guinea", "PH": "Philippines", "PK": "Pakistan",
    "PL": "Poland", "PR": "Puerto Rico", "PS": "Palestine",
    "PT": "Portugal", "PW": "Palau", "PY": "Paraguay", "QA": "Qatar",
    "RE": "Réunion", "RO": "Romania", "RS": "Serbia", "RU": "Russia",
    "RW": "Rwanda", "SA": "Saudi Arabia", "SB": "Solomon Islands",
    "SC": "Seychelles", "SD": "Sudan", "SE": "Sweden", "SG": "Singapore",
    "SI": "Slovenia", "SK": "Slovakia", "SL": "Sierra Leone",
    "SM": "San Marino", "SN": "Senegal", "SO": "Somalia",
    "SR": "Suriname", "SS": "South Sudan", "ST": "São Tomé and Príncipe",
    "SU": "Soviet Union",  # MB still emits it for Soviet-era artists
    "SV": "El Salvador", "SY": "Syria", "SZ": "Eswatini", "TD": "Chad",
    "TG": "Togo", "TH": "Thailand", "TJ": "Tajikistan",
    "TL": "Timor-Leste", "TM": "Turkmenistan", "TN": "Tunisia",
    "TO": "Tonga", "TR": "Turkey", "TT": "Trinidad and Tobago",
    "TV": "Tuvalu", "TW": "Taiwan", "TZ": "Tanzania", "UA": "Ukraine",
    "UG": "Uganda", "US": "United States", "UY": "Uruguay",
    "UZ": "Uzbekistan", "VA": "Vatican City",
    "VC": "Saint Vincent and the Grenadines", "VE": "Venezuela",
    "VG": "British Virgin Islands", "VN": "Vietnam", "VU": "Vanuatu",
    "WS": "Samoa", "XK": "Kosovo", "YE": "Yemen", "ZA": "South Africa",
    "ZM": "Zambia", "ZW": "Zimbabwe",
}

# ── Synonyms, cities and subdivisions → country ──────────────────────────────
# Every key below was OBSERVED in the pool (2026-08-11 audit) — this is a
# curated cleanup of real drift, not an attempt at a gazetteer. Keys are
# matched case-insensitively and diacritic-folded.
ALIASES = {
    # Country-name synonyms
    "usa": "United States", "u.s.": "United States", "u.s.a.": "United States",
    "united states of america": "United States", "america": "United States",
    "uk": "United Kingdom", "great britain": "United Kingdom",
    "england": "United Kingdom", "scotland": "United Kingdom",
    "wales": "United Kingdom", "northern ireland": "United Kingdom",
    "the netherlands": "Netherlands", "holland": "Netherlands",
    "czech republic": "Czechia", "turkiye": "Turkey",
    "russian federation": "Russia", "kingdom of norway": "Norway",
    "myanmar (burma)": "Myanmar", "burma": "Myanmar",
    "cote d'ivoire": "Ivory Coast", "cote d’ivoire": "Ivory Coast",
    "trinidad": "Trinidad and Tobago", "bosnia": "Bosnia and Herzegovina",
    "the bahamas": "Bahamas", "uae": "United Arab Emirates",
    "republic of the congo": "Congo",
    "democratic republic of the congo": "DR Congo",
    "south georgia and the south sand": "Unknown",  # truncated joke location
    "south georgia and the south sandwich islands": "Unknown",
    "ascension and tris": "Unknown", "antarctica": "Unknown",
    "[worldwide]": "Unknown", "worldwide": "Unknown",
    # United States — cities, states, D.C.
    "washington, d.c.": "United States", "d.c.": "United States",
    "texas": "United States", "los angeles": "United States",
    "new york": "United States", "chicago": "United States",
    "boston": "United States", "minneapolis": "United States",
    "san francisco": "United States", "portland": "United States",
    "atlanta": "United States", "san diego": "United States",
    "philadelphia": "United States", "pittsburgh": "United States",
    "austin": "United States", "new orleans": "United States",
    "nashville": "United States", "orlando": "United States",
    "santa barbara": "United States", "asheville": "United States",
    "madison": "United States", "houston": "United States",
    "seattle": "United States", "long beach": "United States",
    "albuquerque": "United States", "baltimore": "United States",
    "miami": "United States", "durham": "United States",
    "newark": "United States", "tulsa": "United States",
    "saint paul": "United States", "salt lake city": "United States",
    "detroit": "United States", "brooklyn": "United States",
    "bronx": "United States", "virginia beach": "United States",
    "eau claire": "United States", "sacramento": "United States",
    "kansas city": "United States", "denver": "United States",
    "monterey": "United States", "oakland": "United States",
    "tampa": "United States", "phoenixville": "United States",
    "newburgh": "United States",
    # Canada — provinces + cities
    "british columbia": "Canada",
    "ontario": "Canada", "quebec": "Canada", "alberta": "Canada",
    "nova scotia": "Canada", "manitoba": "Canada", "saskatchewan": "Canada",
    "new brunswick": "Canada", "newfoundland and labrador": "Canada",
    "prince edward island": "Canada", "northwest territories": "Canada",
    "yukon": "Canada", "toronto": "Canada", "vancouver": "Canada",
    "montreal": "Canada", "edmonton": "Canada", "longueuil": "Canada",
    # United Kingdom — cities
    "london": "United Kingdom", "greater london": "United Kingdom",
    "bristol": "United Kingdom", "manchester": "United Kingdom",
    "brighton": "United Kingdom", "leeds": "United Kingdom",
    "sheffield": "United Kingdom", "glasgow": "United Kingdom",
    "edinburgh": "United Kingdom", "birmingham": "United Kingdom",
    "oxford": "United Kingdom", "coventry": "United Kingdom",
    "aberdeen": "United Kingdom", "belfast": "United Kingdom",
    "somerset": "United Kingdom", "thamesmead": "United Kingdom",
    "guernsey": "United Kingdom",
    # Ukraine — cities + oblasts (MB areas)
    "kyiv": "Ukraine", "kharkiv": "Ukraine", "kharkivs'ka oblast'": "Ukraine",
    "dnipropetrovs'ka oblast'": "Ukraine", "donets'ka oblast'": "Ukraine",
    "luhans'ka oblast'": "Ukraine", "khersons'ka oblast'": "Ukraine",
    "zhytomyrs'ka oblast'": "Ukraine", "mykolaivs'ka oblast'": "Ukraine",
    "odessa": "Ukraine", "lviv": "Ukraine", "lutsk": "Ukraine",
    "vinnytsia": "Ukraine", "poltava": "Ukraine", "chernivtsi": "Ukraine",
    "brovary": "Ukraine", "sevastopol'": "Ukraine",
    "avtonomna respublika krym": "Ukraine",
    # Caucasus
    "tbilisi": "Georgia", "batumi": "Georgia", "guria": "Georgia",
    "abkhazia": "Georgia",
    "yerevan": "Armenia", "ararat": "Armenia", "aragacotn": "Armenia",
    "vayoc jor": "Armenia", "baki": "Azerbaijan", "baku": "Azerbaijan",
    # Africa — cities + subdivisions
    "lagos": "Nigeria", "abuja": "Nigeria",
    "abuja federal capital territory": "Nigeria", "port harcourt": "Nigeria",
    "warri": "Nigeria", "enugu": "Nigeria", "kaduna": "Nigeria",
    "bayelsa": "Nigeria", "edo": "Nigeria", "abia": "Nigeria",
    "adamawa": "Nigeria",
    "accra": "Ghana", "kumasi": "Ghana", "cape coast": "Ghana",
    "tema": "Ghana",
    "nairobi": "Kenya", "dakar": "Senegal", "matam": "Senegal",
    "bamako": "Mali", "segou": "Mali", "tombouctou": "Mali",
    "dar es salaam": "Tanzania", "morogoro": "Tanzania",
    "zanzibar city": "Tanzania",
    "maputo": "Mozambique", "nampula": "Mozambique",
    "antananarivo": "Madagascar", "cairo": "Egypt",
    "johannesburg": "South Africa", "al qadarif": "Sudan",
    # Middle East
    "beirut": "Lebanon", "beyrouth": "Lebanon",
    "damascus": "Syria", "aleppo": "Syria", "al ladhiqiyah": "Syria",
    "baghdad": "Iraq", "arbil": "Iraq", "as sulaymaniyah": "Iraq",
    "sulaymaniyah": "Iraq", "dubai": "United Arab Emirates",
    "san‘a’": "Yemen", "sana'a": "Yemen", "ibb": "Yemen",
    # Bangladesh — cities/districts
    "dhaka": "Bangladesh", "tangail": "Bangladesh", "bogra": "Bangladesh",
    "gazipur": "Bangladesh", "khulna": "Bangladesh",
    "rajshahi": "Bangladesh", "chittagong": "Bangladesh",
    "brahmanbaria": "Bangladesh", "kishoreganj": "Bangladesh",
    # Europe — cities
    "berlin": "Germany", "hamburg": "Germany", "bremen": "Germany",
    "cologne": "Germany", "dresden": "Germany", "stuttgart": "Germany",
    "bochum": "Germany", "lahr": "Germany",
    "paris": "France", "lyon": "France", "rennes": "France",
    "amsterdam": "Netherlands", "rotterdam": "Netherlands",
    "brussels": "Belgium", "stockholm": "Sweden", "nykoping": "Sweden",
    "bergen": "Norway", "lisboa": "Portugal", "madrid": "Spain",
    "barcelona": "Spain", "wien": "Austria", "zurich": "Switzerland",
    "moscow": "Russia", "reggio emilia": "Italy", "mantova": "Italy",
    # Latin America — cities + states
    "sao paulo": "Brazil", "rio de janeiro": "Brazil",
    "minas gerais": "Brazil", "ceara": "Brazil", "recife": "Brazil",
    "curitiba": "Brazil", "florianopolis": "Brazil",
    "nova friburgo": "Brazil", "atibaia": "Brazil",
    "la paz": "Bolivia", "cochabamba": "Bolivia", "potosi": "Bolivia",
    "chuquisaca": "Bolivia",
    "asuncion": "Paraguay",
    "tegucigalpa": "Honduras", "cortes": "Honduras", "valle": "Honduras",
    "islas de la bahia": "Honduras",
    "san salvador": "El Salvador",
    # Oceania / Asia — cities
    "sydney": "Australia", "melbourne": "Australia",
    "adelaide": "Australia", "gold coast": "Australia",
    "wellington": "New Zealand", "yichang": "China",
}


# Letters NFD can't reduce — they are distinct characters, not base+mark
# ('Bakı' stayed 'bakı' and missed its alias until ı→i was added).
_SPECIAL = str.maketrans({"ı": "i", "ø": "o", "đ": "d", "ł": "l",
                          "ß": "ss", "æ": "ae", "œ": "oe", "þ": "th"})


def _fold(s: str) -> str:
    s = s.lower().translate(_SPECIAL)
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if not unicodedata.combining(c)).strip()


# Folded country name → canonical capitalization ('belize' → 'Belize').
# Bandcamp locations are free text typed by artists, so the same country
# arrives in every casing; without this only ALIASED spellings healed.
_NAME_CANON = {}
for _name in ISO2.values():
    _NAME_CANON["".join(c for c in unicodedata.normalize("NFD", _name.lower())
                        if not unicodedata.combining(c))] = _name


def canonical_region(value) -> str:
    """The canonical spelling for a region value; '' for empty input.

    Maps what it knows (ISO-2 codes, country names in any casing, observed
    synonyms/cities/subdivisions) and passes everything else through
    trimmed — never guesses.
    """
    if not value:
        return ""
    v = str(value).strip()
    if len(v) == 2 and v.upper() in ISO2:
        return ISO2[v.upper()]
    f = _fold(v)
    return ALIASES.get(f) or _NAME_CANON.get(f) or v
