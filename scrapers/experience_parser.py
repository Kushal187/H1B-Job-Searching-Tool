"""Parse years-of-experience requirements from job descriptions.

Extracts the minimum years of experience mentioned in a job posting's
description text and provides an ATS-aware description extractor.
"""

import re

# ── HTML tag stripper ────────────────────────────────────────────────────────

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    return _HTML_TAG_RE.sub(" ", text)


# ── Experience patterns (compiled once) ──────────────────────────────────────
# Each pattern captures the numeric "floor" of the experience requirement.

_EXPERIENCE_PATTERNS: list[re.Pattern] = [
    # "minimum 3 years" / "at least 2 years" / "min. 5 yrs"
    re.compile(
        r"(?:minimum|at\s+least|min\.?)\s+(\d+)\s*\+?\s*(?:years?|yrs?)",
        re.IGNORECASE,
    ),
    # "3+ years of experience" / "5 years experience" / "2 yrs exp"
    re.compile(
        r"(\d+)\s*\+?\s*(?:years?|yrs?)\s+(?:of\s+)?(?:experience|exp\.?)",
        re.IGNORECASE,
    ),
    # "3-5 years of experience" / "3 to 5 years of full-time QA experience"
    re.compile(
        r"(\d+)\s*[-–to]+\s*\d+\s*(?:years?|yrs?)\s+(?:of\s+)?(?:[\w-]+\s+){0,4}(?:experience|exp\.?)",
        re.IGNORECASE,
    ),
    # "3+ years" near "experience" within same sentence
    re.compile(
        r"(\d+)\s*\+\s*(?:years?|yrs?)",
        re.IGNORECASE,
    ),
]


def parse_experience_years(text: str) -> int | None:
    """Extract minimum years of experience from job description text.

    Returns the lowest number found across all experience-related phrases,
    or ``None`` if no experience requirement is mentioned.
    """
    if not text:
        return None

    text = _strip_html(text)
    found: list[int] = []

    for pat in _EXPERIENCE_PATTERNS:
        for m in pat.finditer(text):
            val = int(m.group(1))
            if 0 <= val <= 20:
                found.append(val)

    return min(found) if found else None


# ── ATS-aware description extractor ─────────────────────────────────────────


def extract_description(raw_json: dict, ats_system: str) -> str | None:
    """Pull plaintext description from a job's raw_json based on ATS type.

    Returns ``None`` when the ATS does not provide descriptions in the
    list-level API response (e.g. Workday).
    """
    if ats_system == "lever":
        return raw_json.get("descriptionPlain") or raw_json.get("descriptionBodyPlain")
    if ats_system == "ashby":
        return raw_json.get("descriptionPlain")
    if ats_system == "greenhouse":
        content = raw_json.get("content")
        return _strip_html(content) if content else None
    # workday and unknown — no description available
    return None


