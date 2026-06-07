"""DIG — Region → Spotify-market mapping.

Single source of truth for the macro-region → market-code map used by the
discovery scripts (discover.py, discover_artists.py). Previously copy-pasted;
the discover_artists copy had drifted — e.g. Tibet -> ["CN"], an invalid
Spotify market that silently returned zero results.
"""

# ── Region → Market mapping ──
# Maps region label → Spotify market codes used for search filtering.
# Where a country has no Spotify market (CN, CU, ER, SO, SS, TM, AF…) a
# geographically/culturally close proxy is used instead — searches still
# return music from the target culture, just licensed via the proxy market.
# Invalid codes fail silently (safe_call catches API errors → 0 results).
REGIONS = {
    # ── Western Europe ────────────────────────────────────────────────────────
    "UK":               ["GB"],
    "Ireland":          ["IE"],
    "France":           ["FR"],
    "Germany":          ["DE"],
    "Austria":          ["AT"],
    "Switzerland":      ["CH"],
    "Italy":            ["IT"],
    "Spain":            ["ES"],
    "Portugal":         ["PT"],
    "Netherlands":      ["NL"],
    "Belgium":          ["BE"],
    "Nordic":           ["SE", "NO", "FI", "DK", "IS"],

    # ── Eastern & Central Europe ──────────────────────────────────────────────
    "Baltic States":    ["LV", "LT", "EE"],
    "Eastern Europe":   ["PL", "CZ", "HU", "SK", "RO", "BG"],
    "Ukraine":          ["UA"],
    "Russia":           ["RU"],
    # Balkans: Serbia, Croatia, Bosnia, Slovenia, Montenegro, N. Macedonia, Albania
    "Balkans":          ["RS", "HR", "BA", "SI", "ME", "MK", "AL"],
    "Greece":           ["GR"],

    # ── Caucasus ─────────────────────────────────────────────────────────────
    # Georgia (polyphonic singing), Armenia (duduk), Azerbaijan (mugham)
    "Caucasus":         ["GE", "AM", "AZ"],

    # ── Middle East & North Africa ────────────────────────────────────────────
    "Turkey":           ["TR"],
    "Iran":             ["IR"],
    # Egypt split out — Um Kulthum, sha'bi, mahraganat deserve their own cell budget
    "Egypt":            ["EG"],
    "North Africa":     ["MA", "DZ", "TN", "LY"],
    # Sudan + South Sudan (Nubian, Sudanese jazz, zar)
    "Sudan":            ["SD", "SS"],
    # Gulf + Levant + Iraq
    "Middle East":      ["SA", "AE", "LB", "JO", "IQ", "IL", "OM", "KW", "QA", "BH", "YE"],

    # ── Central Asia ─────────────────────────────────────────────────────────
    # KZ=Kazakhstan, UZ=Uzbekistan, KG=Kyrgyzstan, TJ=Tajikistan, TM=Turkmenistan
    "Central Asia":     ["KZ", "UZ", "KG", "TJ", "TM"],
    # Afghanistan: AF not a Spotify market — Pakistan/Tajikistan as proxy
    "Afghanistan":      ["PK", "TJ"],

    # ── East Asia ────────────────────────────────────────────────────────────
    "Japan":            ["JP"],
    "South Korea":      ["KR"],
    # CN not a Spotify market — TW/HK proxy for Mandopop, C-pop, guqin, etc.
    "China":            ["TW", "HK"],
    "Hong Kong":        ["HK"],
    "Taiwan":           ["TW"],
    "Mongolia":         ["MN"],
    # CN not a Spotify market — Nepal/India proxy for Tibetan music
    "Tibet":            ["NP", "IN"],

    # ── Southeast Asia ───────────────────────────────────────────────────────
    "Thailand":         ["TH"],
    "Vietnam":          ["VN"],
    "Laos":             ["LA"],
    "Myanmar":          ["MM"],
    "Cambodia":         ["KH"],
    "Indonesia":        ["ID"],
    "Philippines":      ["PH"],
    "Malaysia":         ["MY"],
    "Singapore":        ["SG"],

    # ── South Asia ───────────────────────────────────────────────────────────
    "India":            ["IN"],
    "Nepal":            ["NP"],
    # Pakistan, Bangladesh, Sri Lanka
    "South Asia":       ["PK", "BD", "LK"],

    # ── West Africa ──────────────────────────────────────────────────────────
    # Nigeria, Ghana, Senegal, Côte d'Ivoire, Guinea, Burkina Faso, Togo, Benin,
    # Sierra Leone, Liberia
    "West Africa":      ["NG", "GH", "SN", "CI", "GN", "BF", "TG", "BJ", "SL", "LR"],
    # Sahel: Mali (griot heartland, desert blues), Mauritania (moorish music),
    # Niger, Gambia
    "Sahel":            ["ML", "MR", "NE", "GM"],
    # Cape Verde: morna — one of the most distinctive island music cultures on earth
    "Cape Verde":       ["CV"],

    # ── Central Africa ───────────────────────────────────────────────────────
    # DR Congo (rumba, soukous), Cameroon, Republic of Congo, Angola (semba/kizomba),
    # Gabon, Equatorial Guinea, Central African Republic
    "Central Africa":   ["CD", "CM", "CG", "AO", "GA", "GQ", "CF"],

    # ── East Africa ──────────────────────────────────────────────────────────
    # Kenya, Tanzania, Uganda, Rwanda, Burundi, Mozambique (marrabenta, timbila)
    "East Africa":      ["KE", "TZ", "UG", "RW", "BI", "MZ"],
    # Horn of Africa: Ethiopia (jazz, azmari, tizita), Somalia (proxy KE/DJ),
    # Djibouti, Eritrea (proxy ET)
    "Horn of Africa":   ["ET", "DJ", "KE"],
    # Madagascar: valiha, salegy — totally isolated island tradition
    "Madagascar":       ["MG"],

    # ── Southern Africa ──────────────────────────────────────────────────────
    # South Africa, Zimbabwe, Zambia, Malawi, Botswana, Namibia, Eswatini
    "Southern Africa":  ["ZA", "ZW", "ZM", "MW", "BW", "NA", "SZ"],

    # ── Americas ─────────────────────────────────────────────────────────────
    "USA":              ["US"],
    "Canada":           ["CA"],
    "Mexico":           ["MX"],
    # CU not a Spotify market — Dominican Republic / Jamaica as proxy for son,
    # rumba, nueva trova, timba
    "Cuba":             ["DO", "JM"],
    # Caribbean: Jamaica, Trinidad, Dominican Rep, Haiti, Barbados, Saint Lucia,
    # Saint Vincent, Grenada, Antigua, Bahamas, Belize
    "Caribbean":        ["JM", "TT", "DO", "HT", "BB", "LC", "VC", "GD", "AG", "BS", "BZ"],
    # Central America: Costa Rica, Panama, Guatemala, Honduras, El Salvador, Nicaragua
    "Central America":  ["CR", "PA", "GT", "HN", "SV", "NI"],
    "Colombia":         ["CO"],
    "Brazil":           ["BR"],
    "Argentina":        ["AR"],
    "Chile":            ["CL"],
    "Peru":             ["PE"],
    # South America: Ecuador, Venezuela, Bolivia, Paraguay, Uruguay, Guyana, Suriname
    "South America":    ["EC", "VE", "BO", "PY", "UY", "GY", "SR"],

    # ── Oceania ──────────────────────────────────────────────────────────────
    "Australia":        ["AU"],
    "New Zealand":      ["NZ"],
    # Melanesia & Polynesia: Fiji, PNG, Samoa, Tonga, Solomons, Vanuatu,
    # Micronesia, Palau, Marshall Islands, Kiribati
    "Pacific Islands":  ["FJ", "PG", "WS", "TO", "SB", "VU", "FM", "PW", "MH", "KI"],
}

