"""Priority scoring logic for matched companies.

Assigns a priority score to each matched company based on:
  - Source overlap (in both SEC + H1B data)
  - H1B approval volume
  - SEC fundraising amount
  - Recency of H1B filings
"""

import math

from db import database


def score_company(company: dict) -> float:
    """Calculate priority score for a single company.

    Scoring formula:
      +50 if company appears in BOTH SEC and H1B data
      + min(30, approval_count * 0.5) for H1B approval volume
      + min(10, log10(sec_amount) - 4) for fundraising (scaled)
      +10 if H1B filing in FY2025 (recency bonus)

    Args:
        company: Dict with matched_companies fields.

    Returns:
        Priority score (higher = better target).
    """
    score = 0.0

    # Source overlap bonus
    if company.get("source") == "both":
        score += 50.0

    # H1B approval volume
    approvals = company.get("h1b_approval_count", 0) or 0
    score += min(30.0, approvals * 0.5)

    # SEC fundraising amount (log-scaled)
    amount = company.get("sec_amount_raised", 0) or 0
    if amount > 0:
        try:
            log_score = math.log10(amount) - 4  # $10K = 0, $1M = 2, $100M = 4
            score += min(10.0, max(0.0, log_score))
        except (ValueError, OverflowError):
            pass

    # Recency bonus — check if H1B filing is from FY2025
    # We look this up from h1b_sponsors by normalized name
    normalized = company.get("normalized_name", "")
    if normalized:
        recent = database.query(
            "SELECT fiscal_year FROM h1b_sponsors WHERE normalized_name = ? ORDER BY fiscal_year DESC LIMIT 1",
            (normalized,),
        )
        if recent and recent[0].get("fiscal_year", "").endswith("2025"):
            score += 10.0

    return round(score, 2)


def update_priority_scores():
    """Batch-update priority_score for all matched companies."""
    print("Computing priority scores...")

    companies = database.query("SELECT * FROM matched_companies")

    if not companies:
        print("  No matched companies to score.")
        return

    updates = []
    for company in companies:
        score = score_company(company)
        updates.append((score, company["id"]))

    with database.get_db() as conn:
        conn.executemany(
            "UPDATE matched_companies SET priority_score = ? WHERE id = ?",
            updates,
        )

    # Print summary stats
    scored = database.query(
        "SELECT priority_score FROM matched_companies ORDER BY priority_score DESC"
    )
    scores = [r["priority_score"] for r in scored]

    if scores:
        print(f"  Scored {len(scores)} companies")
        print(f"  Score range: {min(scores):.1f} — {max(scores):.1f}")
        print(f"  Mean score:  {sum(scores) / len(scores):.1f}")
        top = database.query(
            "SELECT company_name, priority_score FROM matched_companies ORDER BY priority_score DESC LIMIT 10"
        )
        print(f"  Top 10 companies:")
        for i, c in enumerate(top, 1):
            print(f"    {i:2d}. {c['company_name'][:50]:<50s} score={c['priority_score']:.1f}")
