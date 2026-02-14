"""Company name normalization for matching and API queries."""

import re


def normalize_company_name(name: str) -> str:
    """Normalize company name for matching and API queries.

    Strips common corporate suffixes, removes punctuation and special
    characters, and lowercases the result to produce a slug suitable
    for both fuzzy matching and ATS board URL lookups.

    Args:
        name: Raw company name string.

    Returns:
        Normalized lowercase slug (e.g. "stripe", "airbnb").
    """
    suffixes = [
        r',?\s+Inc\.?$',
        r',?\s+LLC\.?$',
        r',?\s+Corp\.?$',
        r',?\s+Corporation$',
        r',?\s+Ltd\.?$',
        r',?\s+Limited$',
        r',?\s+LP\.?$',
        r',?\s+LLP\.?$',
        r',?\s+Co\.?$',
        r',?\s+Company$',
        r',?\s+Group$',
        r',?\s+Holdings?$',
        r',?\s+Technologies$',
        r',?\s+Technology$',
        r',?\s+Solutions$',
        r',?\s+Services$',
        r',?\s+International$',
        r',?\s+Enterprises?$',
    ]
    cleaned = name.strip()
    for suffix in suffixes:
        cleaned = re.sub(suffix, '', cleaned, flags=re.IGNORECASE)
    # Remove punctuation and special chars for API slug
    slug = re.sub(r'[,.\s\-&\'\"()]', '', cleaned)
    return slug.lower()
