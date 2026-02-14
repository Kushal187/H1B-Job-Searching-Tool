"""Job title filter for entry-level / new-grad tech roles.

Filters job titles to only include entry-level positions in the
specified engineering and product disciplines.
"""

import re

# ── Role patterns (must match at least one) ──────────────────────────────────
# Each pattern is compiled case-insensitively.

ROLE_PATTERNS: list[re.Pattern] = [
    # ── Software Engineering ──────────────────────────────────────────────
    re.compile(r"software\s+engineer", re.IGNORECASE),
    re.compile(r"software\s+engineering", re.IGNORECASE),      # "Software Engineering New Grad"
    re.compile(r"software\s+development\s+engineer", re.IGNORECASE),
    re.compile(r"software\s+developer", re.IGNORECASE),
    re.compile(r"\bsde\b", re.IGNORECASE),                     # SDE
    re.compile(r"\bswe\b", re.IGNORECASE),                     # SWE

    # ── Backend / Frontend ────────────────────────────────────────────────
    re.compile(r"backend\s+engineer", re.IGNORECASE),
    re.compile(r"backend\s+developer", re.IGNORECASE),
    re.compile(r"frontend\s+engineer", re.IGNORECASE),
    re.compile(r"frontend\s+developer", re.IGNORECASE),
    re.compile(r"front[\s\-]?end\s+engineer", re.IGNORECASE),
    re.compile(r"front[\s\-]?end\s+developer", re.IGNORECASE),

    # ── Full Stack ────────────────────────────────────────────────────────
    re.compile(r"full[\s\-]?stack", re.IGNORECASE),            # fullstack / full stack / full-stack

    # ── Data ──────────────────────────────────────────────────────────────
    re.compile(r"data\s+engineer", re.IGNORECASE),
    re.compile(r"data\s+scientist", re.IGNORECASE),
    re.compile(r"analytics\s+engineer", re.IGNORECASE),

    # ── AI / ML ───────────────────────────────────────────────────────────
    re.compile(r"\bai\s+engineer", re.IGNORECASE),             # AI Engineer
    re.compile(r"\bai[/\s]ml\s+engineer", re.IGNORECASE),     # AI/ML Engineer
    re.compile(r"gen[\s\-]?ai\s+engineer", re.IGNORECASE),    # GenAI Engineer
    re.compile(r"machine\s+learning\s+engineer", re.IGNORECASE),
    re.compile(r"\bml\s+engineer", re.IGNORECASE),
    re.compile(r"applied\s+ai\s+engineer", re.IGNORECASE),    # Applied AI Engineer
    re.compile(r"applied\s+ml\s+engineer", re.IGNORECASE),    # Applied ML Engineer
    re.compile(r"applied\s+scientist", re.IGNORECASE),         # Applied Scientist
    re.compile(r"research\s+engineer", re.IGNORECASE),
    re.compile(r"research\s+scientist", re.IGNORECASE),

    # ── Forward Deployed ──────────────────────────────────────────────────
    re.compile(r"forward\s+deploy", re.IGNORECASE),            # forward deployed / forward deployment
    re.compile(r"\bfde\b", re.IGNORECASE),                     # FDE abbreviation

    # ── Mobile Engineering ────────────────────────────────────────────────
    re.compile(r"mobile\s+engineer", re.IGNORECASE),
    re.compile(r"mobile\s+developer", re.IGNORECASE),
    re.compile(r"\bios\s+engineer", re.IGNORECASE),
    re.compile(r"\bios\s+developer", re.IGNORECASE),
    re.compile(r"\bandroid\s+engineer", re.IGNORECASE),
    re.compile(r"\bandroid\s+developer", re.IGNORECASE),

    # ── Cloud / Infra / DevOps ────────────────────────────────────────────
    re.compile(r"cloud\s+engineer", re.IGNORECASE),
    re.compile(r"cloud\s+developer", re.IGNORECASE),
    re.compile(r"devops\s+engineer", re.IGNORECASE),
    re.compile(r"infrastructure\s+engineer", re.IGNORECASE),
    re.compile(r"platform\s+engineer", re.IGNORECASE),
    re.compile(r"site\s+reliability\s+engineer", re.IGNORECASE),
    re.compile(r"\bsre\b", re.IGNORECASE),                    # SRE
    re.compile(r"systems\s+engineer", re.IGNORECASE),
    re.compile(r"network\s+engineer", re.IGNORECASE),
    re.compile(r"reliability\s+engineer", re.IGNORECASE),

    # ── Security ──────────────────────────────────────────────────────────
    re.compile(r"security\s+engineer", re.IGNORECASE),
    re.compile(r"cybersecurity\s+engineer", re.IGNORECASE),

    # ── Embedded / Hardware / Robotics ────────────────────────────────────
    re.compile(r"embedded\s+engineer", re.IGNORECASE),
    re.compile(r"embedded\s+software", re.IGNORECASE),
    re.compile(r"firmware\s+engineer", re.IGNORECASE),
    re.compile(r"robotics\s+engineer", re.IGNORECASE),
    re.compile(r"hardware\s+engineer", re.IGNORECASE),

    # ── QA / Test ─────────────────────────────────────────────────────────
    re.compile(r"\bqa\s+engineer", re.IGNORECASE),
    re.compile(r"\bsdet\b", re.IGNORECASE),
    re.compile(r"test\s+engineer", re.IGNORECASE),
    re.compile(r"quality\s+engineer", re.IGNORECASE),
    re.compile(r"automation\s+engineer", re.IGNORECASE),

    # ── Solutions Engineering ─────────────────────────────────────────────
    re.compile(r"solutions\s+engineer", re.IGNORECASE),
    re.compile(r"solutions\s+architect", re.IGNORECASE),
    re.compile(r"solutions\s+consultant", re.IGNORECASE),
    re.compile(r"integration\s+engineer", re.IGNORECASE),

    # ── Product Management ────────────────────────────────────────────────
    re.compile(r"product\s+manager", re.IGNORECASE),
    re.compile(r"\bapm\b", re.IGNORECASE),                     # Associate Product Manager
    re.compile(r"technical\s+program\s+manager", re.IGNORECASE),
    re.compile(r"\btpm\b", re.IGNORECASE),                     # TPM

    # ── Other Engineering ─────────────────────────────────────────────────
    re.compile(r"database\s+engineer", re.IGNORECASE),
    re.compile(r"release\s+engineer", re.IGNORECASE),
    re.compile(r"build\s+engineer", re.IGNORECASE),
    re.compile(r"support\s+engineer", re.IGNORECASE),
    re.compile(r"implementation\s+engineer", re.IGNORECASE),
]

# ── Senior-level exclusions (reject if any match) ────────────────────────────
# Titles containing these are considered above entry-level.

SENIOR_PATTERNS: list[re.Pattern] = [
    re.compile(r"\bsenior\b", re.IGNORECASE),
    re.compile(r"\bsr\.?\b", re.IGNORECASE),                   # Sr. / Sr (including in parens)
    re.compile(r"\bstaff\b", re.IGNORECASE),
    re.compile(r"\bprincipal\b", re.IGNORECASE),
    re.compile(r"\bdirector\b", re.IGNORECASE),
    re.compile(r"\blead\b", re.IGNORECASE),
    re.compile(r"\bleader\b", re.IGNORECASE),                  # "Engineering Leader"
    re.compile(r"\bhead\b", re.IGNORECASE),
    re.compile(r"\bvp\b", re.IGNORECASE),
    re.compile(r"\bvice\s+president\b", re.IGNORECASE),
    re.compile(r"\bintern\b", re.IGNORECASE),                  # internships
    re.compile(r"\bphd\b", re.IGNORECASE),                     # PhD-specific roles
    re.compile(r"\bii\b", re.IGNORECASE),                      # Level II / Engineer II
    re.compile(r"\biii\b", re.IGNORECASE),                     # Level III
    re.compile(r"\b[2-9]\b", re.IGNORECASE),                   # Level 2+ (but not 1)
    # Management titles (but NOT "Product Manager" or "Program Manager" — those are roles)
    re.compile(r"engineering\s+manager", re.IGNORECASE),       # "Software Engineering Manager"
    re.compile(r"^Manager[,\s]", re.IGNORECASE),               # "Manager, Software Engineering" (at start)
    re.compile(r"\bgroup\s+product\s+manager", re.IGNORECASE), # Group PM is senior
]


def is_target_role(title: str) -> bool:
    """Return True if *title* is an entry-level / new-grad tech role.

    A title passes if it:
    1. Matches at least one ``ROLE_PATTERN`` (right discipline), AND
    2. Does NOT match any ``SENIOR_PATTERN`` (not above entry-level).

    Only strong entry-level signals ("new grad", "entry level", "junior")
    can override a senior-level keyword. Weak signals like Roman numeral "I"
    or "Associate" do NOT override (since "Senior Engineer I" and
    "Associate Principal" are still senior roles).
    """
    if not title:
        return False

    # Step 1 — must match at least one target role
    if not any(pat.search(title) for pat in ROLE_PATTERNS):
        return False

    is_senior = any(pat.search(title) for pat in SENIOR_PATTERNS)

    # Step 2 — strong entry-level signals override seniority
    # "New Grad", "Entry Level", "Junior" are unambiguous — always pass
    strong_entry = re.compile(
        r"new\s*grad|entry[\s\-]?level|\bjunior\b",
        re.IGNORECASE,
    )
    if strong_entry.search(title):
        return True

    # Step 3 — reject senior-level titles
    if is_senior:
        return False

    # Step 4 — weak entry-level signals pass only if NOT senior
    # "Associate", level "I"/"1", "L1" etc.
    weak_entry = re.compile(
        r"\bassociate\b|\b[I1]\b(?!\s*[-–])|level\s*1|\bL1\b|\bE1\b",
        re.IGNORECASE,
    )
    if weak_entry.search(title):
        return True

    # No seniority indicator → likely entry / mid-level → keep it
    return True
