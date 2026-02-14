"""USA location filter for job listings.

Provides heuristic matching to determine whether a job location string
refers to a position based in the United States.
"""

import re

# All 50 US states + DC variants
US_STATES: set[str] = {
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming",
    "Washington, D.C.", "Washington D.C.",
}

# Two-letter US state abbreviations + DC
US_STATE_ABBREVS: set[str] = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
}

# Keywords that directly indicate a US location (case-insensitive check)
US_KEYWORDS: set[str] = {
    "united states",
    "usa",
    "u.s.a.",
    "u.s.",
}

# Regex patterns that indicate US location
US_KEYWORD_PATTERNS: list[re.Pattern] = [
    re.compile(r"\bUS\b", re.IGNORECASE),                 # "US", "US-Remote", "Remote in US"
    re.compile(r"\bUS[-\s]?Remote\b", re.IGNORECASE),     # "US-Remote", "US Remote"
    re.compile(r"Remote\s*[-–,]\s*US\b", re.IGNORECASE),  # "Remote - US", "Remote, US"
    re.compile(r"Remote\s+in\s+(the\s+)?US\b", re.IGNORECASE),  # "Remote in the US", "Remote in US"
    re.compile(r"\bRemote\s*[-–,]\s*USA\b", re.IGNORECASE),     # "Remote - USA", "Remote, USA"
    re.compile(r"\bRemote,\s*USA\b", re.IGNORECASE),             # "Remote, USA"
]

# Regional US designations (e.g. Greenhouse custom fields)
US_REGIONS: set[str] = {
    "central - united states",
    "northeast - united states",
    "southeast - united states",
    "southwest - united states",
    "west coast - united states",
    "usca",
}

# Major US cities that often appear WITHOUT state name
# These are unambiguous — no major international city shares these names
US_CITIES: set[str] = {
    # Top metros
    "new york city", "new york", "nyc",
    "los angeles", "la",
    "chicago",
    "houston",
    "phoenix",
    "philadelphia",
    "san antonio",
    "san diego",
    "dallas",
    "austin",
    "jacksonville",
    "san jose",
    "san francisco", "sf",
    "seattle", "sea",
    "denver",
    "boston",
    "nashville",
    "baltimore",
    "oklahoma city",
    "las vegas",
    "portland",
    "memphis",
    "louisville",
    "milwaukee",
    "albuquerque",
    "tucson",
    "fresno",
    "sacramento",
    "mesa",
    "atlanta", "atl",
    "miami",
    "minneapolis",
    "new orleans",
    "cleveland",
    "tampa",
    "st. louis", "st louis",
    "pittsburgh",
    "cincinnati",
    "raleigh",
    "orlando",
    "charlotte",
    "detroit",
    "salt lake city",
    "honolulu",
    "richmond",
    "boise",
    # Common tech hubs
    "silicon valley",
    "mountain view",
    "palo alto",
    "menlo park",
    "sunnyvale",
    "cupertino",
    "santa clara",
    "redmond",
    "bellevue",
    "kirkland",
    "boulder",
    "ann arbor",
    "cambridge",
    "somerville",
    "hoboken",
    "jersey city",
    "brooklyn",
    "manhattan",
    "playa vista",
    "santa monica",
    "culver city",
    "irvine",
    "scottsdale",
    "provo",
    "lehi",
    "reston",
    "arlington",
    "mclean",
    "tysons",
    "bethesda",
    "san mateo",
}

# Locations that mention a US state name but are NOT in the US
NON_US_EXACT: set[str] = {
    "georgia",  # the country, not the state — standalone "Georgia" is ambiguous
}

# Non-US countries/cities to explicitly reject (avoid false positives)
NON_US_INDICATORS: set[str] = {
    "canada", "uk", "united kingdom", "england", "ireland", "germany",
    "france", "india", "japan", "china", "australia", "brazil", "mexico",
    "singapore", "israel", "netherlands", "sweden", "denmark", "finland",
    "switzerland", "romania", "serbia", "poland", "taiwan", "south korea",
    "new zealand", "dubai", "uae",
}


def is_usa_location(location_name: str) -> bool:
    """Return True if *location_name* likely refers to a US-based position.

    The function splits multi-location strings (separated by ``";"``, ``"|"``,
    or ``"/"``), and returns True if **any** part matches a US indicator.
    """
    if not location_name:
        return False

    loc_lower = location_name.lower().strip()

    # Early rejection: if string contains a non-US country/region indicator,
    # reject it to avoid false positives like "Cambridge, UK" or "Richmond, Canada".
    # Use word-boundary regex to avoid rejecting US states like Indiana ("india")
    # or New Mexico ("mexico").
    for indicator in NON_US_INDICATORS:
        if re.search(rf"\b{re.escape(indicator)}\b", loc_lower):
            return False

    # Quick check: entire string matches a keyword pattern
    for pat in US_KEYWORD_PATTERNS:
        if pat.search(location_name):
            return True

    # Quick check: entire string (lowered) directly matches a US keyword
    for kw in US_KEYWORDS:
        if kw in loc_lower:
            return True

    # Quick check: entire string matches a US region
    if loc_lower in US_REGIONS:
        return True

    # Split on common multi-location separators
    parts = re.split(r"[;|]", location_name)

    for part in parts:
        part = part.strip()
        if not part:
            continue

        part_lower = part.lower().strip()

        # Direct region match
        if part_lower in US_REGIONS:
            return True

        # US keyword in this part
        for kw in US_KEYWORDS:
            if kw in part_lower:
                return True

        # Regex patterns
        for pat in US_KEYWORD_PATTERNS:
            if pat.search(part):
                return True

        # Skip standalone country "Georgia"
        if part_lower in NON_US_EXACT:
            continue

        # Check US state full names
        for state in US_STATES:
            if state.lower() in part_lower:
                return True

        # Check US city names — split on commas, slashes, "or", "and"
        sub_parts = re.split(r"[,/]|\bor\b|\band\b", part_lower)
        for sub in sub_parts:
            sub = sub.strip().rstrip(".")
            # Remove common prefixes
            sub = re.sub(r"^(remote\s*[-–]\s*|remote\s+in\s+)", "", sub).strip()

            if sub in US_CITIES:
                return True

            # Check two-letter state abbreviations
            token = sub.upper().strip()
            if token in US_STATE_ABBREVS:
                return True
            # "US-XX" pattern (e.g. "US-NY", "US-Remote")
            if token.startswith("US-") and token[3:] in US_STATE_ABBREVS:
                return True
            # State + zip (e.g. "PA 15213")
            if len(token) >= 2 and token[:2] in US_STATE_ABBREVS:
                rest = token[2:].strip()
                if rest == "" or rest.isdigit():
                    return True

        # Check the full part for "City, ST" patterns like "Pittsburgh, PA"
        segments = [s.strip() for s in part.split(",")]
        for seg in segments:
            seg_upper = seg.strip().upper()
            if seg_upper in US_STATE_ABBREVS:
                return True
            seg_lower = seg.strip().lower()
            if seg_lower in US_CITIES:
                return True

    return False
