"""Text helpers for JD parsing, keyword extraction, and coverage scoring."""

from __future__ import annotations

import re
from collections import Counter

WORD_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+#.-]{1,}")
STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "you",
    "your",
    "our",
    "are",
    "will",
    "this",
    "that",
    "from",
    "have",
    "has",
    "into",
    "about",
    "their",
    "them",
    "they",
    "work",
    "team",
    "role",
    "job",
    "years",
    "year",
}


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_HTML_ENTITY_RE = re.compile(r"&(?:#\d+|#x[\da-fA-F]+|[a-zA-Z]+);")

_ENTITY_MAP = {
    "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"',
    "&apos;": "'", "&nbsp;": " ", "&mdash;": "\u2014", "&ndash;": "\u2013",
    "&rsquo;": "\u2019", "&lsquo;": "\u2018", "&rdquo;": "\u201d",
    "&ldquo;": "\u201c", "&bull;": "\u2022", "&hellip;": "\u2026",
}


def _decode_entity(m: re.Match) -> str:
    ent = m.group(0)
    if ent in _ENTITY_MAP:
        return _ENTITY_MAP[ent]
    if ent.startswith("&#x"):
        return chr(int(ent[3:-1], 16))
    if ent.startswith("&#"):
        return chr(int(ent[2:-1]))
    return ent


def normalize_text(text: str, max_chars: int = 20000) -> str:
    text = (text or "").replace("\r", "\n")
    if _HTML_TAG_RE.search(text):
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</(?:p|div|h[1-6]|li|tr|blockquote)>", "\n", text, flags=re.IGNORECASE)
        text = _HTML_TAG_RE.sub(" ", text)
    if _HTML_ENTITY_RE.search(text):
        text = _HTML_ENTITY_RE.sub(_decode_entity, text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()[:max_chars]


def split_sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+|\n+", text)
    return [p.strip() for p in parts if len(p.strip()) > 20]


def extract_keywords(text: str, top_k: int = 20) -> list[str]:
    words = [w.lower() for w in WORD_RE.findall(text)]
    words = [w for w in words if w not in STOPWORDS and len(w) > 2]
    freq = Counter(words)
    return [w for w, _ in freq.most_common(top_k)]


def _keyword_present(keyword: str, haystack: str) -> bool:
    """Check if a keyword is present in the haystack text.

    For single words, does exact substring match.
    For multi-word phrases, checks that every significant word appears
    somewhere in the haystack (not necessarily adjacent).
    """
    kw = keyword.lower().strip()
    if not kw:
        return True
    if kw in haystack:
        return True
    words = [w for w in kw.split() if len(w) > 2 and w not in STOPWORDS]
    if not words:
        return kw in haystack
    return all(w in haystack for w in words)


def keyword_coverage(keywords: list[str], candidate_text: str) -> tuple[float, list[str]]:
    haystack = (candidate_text or "").lower()
    if not keywords:
        return 1.0, []
    missing = [k for k in keywords if not _keyword_present(k, haystack)]
    covered = len(keywords) - len(missing)
    return round(covered / max(len(keywords), 1), 4), missing
