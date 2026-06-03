"""Filters for roles/companies that require U.S. citizenship or a clearance.

An H1B / visa candidate cannot take roles that require U.S. citizenship,
permanent residency, or an active security clearance.  This module provides
two complementary filters:

* ``requires_us_citizenship(text)`` — scan a single job description for
  citizenship, clearance, or U.S.-person requirements (job-level).
* ``is_excluded_company(name)`` — match a company name against the blocklist of
  defense / intelligence / government-services contractors in
  ``config.EXCLUDED_COMPANY_KEYWORDS`` (company-level; saves scraping egress).
"""

import re

import config

# ── HTML tag stripper (some ATS return HTML descriptions, e.g. Greenhouse) ────

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    return _HTML_TAG_RE.sub(" ", text)


# ── Job-description citizenship / clearance patterns ─────────────────────────
# These fire only on *requirement* phrasing, so boilerplate EEO statements
# ("...without regard to race, religion, citizenship status...") do NOT trigger
# a false exclusion.

_RESTRICTION_PATTERNS: list[re.Pattern] = [
    # ── Explicit U.S. citizenship / residency requirements ──
    re.compile(
        r"\b(?:u\.?\s?s\.?|united\s+states)\s+citizenship\s+(?:is\s+)?"
        r"(?:required|mandatory|needed|necessary)\b",
        re.I,
    ),
    re.compile(
        r"\brequire[ds]?\s+(?:u\.?\s?s\.?|united\s+states)\s+citizen(?:ship)?\b",
        re.I,
    ),
    re.compile(
        r"\bmust\s+be\s+(?:a[n]?\s+)?(?:u\.?\s?s\.?|united\s+states)\s+citizen\b",
        re.I,
    ),
    re.compile(r"\b(?:u\.?\s?s\.?|united\s+states)\s+citizens?\s+only\b", re.I),
    re.compile(
        r"\b(?:restricted|limited|open|available)\s+(?:only\s+)?to\s+"
        r"(?:u\.?\s?s\.?|united\s+states)\s+citizens?\b",
        re.I,
    ),
    re.compile(r"\bcitizenship\s+(?:is\s+)?(?:a\s+)?requirement\b", re.I),

    # ── Security clearance (implies citizenship) ──
    re.compile(r"\bsecurity\s+clearance\b", re.I),
    re.compile(r"\bsecret\s+clearance\b", re.I),
    re.compile(r"\btop[\s-]+secret\b", re.I),
    re.compile(r"\bts\s*/\s*sci\b", re.I),
    re.compile(r"\bsci\s+clearance\b", re.I),
    re.compile(r"\b(?:active|current|existing)\s+clearance\b", re.I),
    re.compile(
        r"\b(?:obtain|maintain)\s+(?:and\s+maintain\s+)?(?:a\s+|an\s+)?"
        r"(?:active\s+|security\s+)?clearance\b",
        re.I,
    ),

    # ── U.S.-person requirement (export-control roles are U.S.-person only) ──
    re.compile(r"\b(?:u\.?\s?s\.?|united\s+states)\s+persons?\b", re.I),
]

# Negation cues — if one appears in the same clause as a match, the match is
# treated as a non-requirement and ignored.  We check the clause *before* the
# match (e.g. "does not require US citizenship") and the clause *after* it
# (e.g. "security clearance is not required" / "...preferred but not required"),
# since real postings put the negation on either side of the phrase.  This
# avoids excluding jobs that explicitly say a clearance/citizenship is NOT
# needed.
_NEGATION_RE = re.compile(
    r"\b(?:no|not|never|without|non|don'?t|does\s*n'?t|do\s*not|"
    r"is\s*n'?t|are\s*n'?t|won'?t|cannot|can'?t)\b",
    re.I,
)
_CLAUSE_BOUNDARY_RE = re.compile(r"[.;:\n\r•|]")


def _is_negated(text: str, start: int, end: int) -> bool:
    """Return True if the clause surrounding the match negates it.

    Looks at both the clause immediately preceding *start* and the clause
    immediately following *end* (each bounded by the nearest clause boundary),
    so phrasings like "security clearance preferred but not required" are
    recognised as non-requirements.
    """
    # Preceding clause: text after the last boundary before the match.
    before = text[:start]
    pre_bounds = [m.end() for m in _CLAUSE_BOUNDARY_RE.finditer(before)]
    pre_clause = before[pre_bounds[-1]:] if pre_bounds else before
    if _NEGATION_RE.search(pre_clause):
        return True

    # Following clause: text up to the first boundary after the match.
    after = text[end:]
    nxt = _CLAUSE_BOUNDARY_RE.search(after)
    post_clause = after[: nxt.start()] if nxt else after
    return bool(_NEGATION_RE.search(post_clause))


def requires_us_citizenship(text: str | None) -> bool:
    """Return True if *text* indicates a citizenship / clearance / ITAR requirement.

    Designed to ignore EEO boilerplate and explicit negations such as
    "no clearance required" or "does not require U.S. citizenship".
    """
    if not text:
        return False
    clean = _strip_html(text)
    for pat in _RESTRICTION_PATTERNS:
        for m in pat.finditer(clean):
            if not _is_negated(clean, m.start(), m.end()):
                return True
    return False


# ── Company-level blocklist ──────────────────────────────────────────────────
# Word-boundary patterns built from config.EXCLUDED_COMPANY_KEYWORDS.  Matching
# is done on the original (spaced) company name so word boundaries work — the
# normalized slug strips spaces, which would let "saic" hit "mosaic".

_EXCLUDED_PATTERNS: list[re.Pattern] = [
    re.compile(rf"\b{re.escape(kw.strip().lower())}\b")
    for kw in config.EXCLUDED_COMPANY_KEYWORDS
    if kw.strip()
]


def is_excluded_company(company_name: str | None) -> bool:
    """Return True if *company_name* is on the citizenship-restricted blocklist."""
    if not company_name:
        return False
    name = company_name.lower()
    return any(pat.search(name) for pat in _EXCLUDED_PATTERNS)
