"""Fuzzy matching pipeline for SEC and H1B company data.

Cross-references SEC Form D filers with known H1B sponsors using
normalized names and fuzzy string matching, then writes the merged
results to the matched_companies table.

Optimized with prefix bucketing + rapidfuzz C++ engine to handle
80K+ x 90K+ name comparisons in minutes instead of hours.
"""

from collections import defaultdict

from rapidfuzz import fuzz, process

import config
from db import database


def _get_unique_sec_companies() -> dict[str, dict]:
    """Get unique SEC companies grouped by normalized name."""
    rows = database.query(
        """
        SELECT company_name, normalized_name, state, industry_group, total_amount_sold
        FROM sec_formd_companies
        WHERE normalized_name IS NOT NULL AND normalized_name != ''
    """
    )

    grouped = {}
    for row in rows:
        key = row["normalized_name"]
        if key not in grouped:
            grouped[key] = {
                "company_name": row["company_name"],
                "normalized_name": key,
                "state": row["state"],
                "total_amount_sold": row["total_amount_sold"] or 0,
            }
        else:
            existing = grouped[key]
            existing["total_amount_sold"] += row["total_amount_sold"] or 0

    return grouped


def _get_unique_h1b_sponsors() -> dict[str, dict]:
    """Get unique H1B sponsors grouped by normalized name."""
    rows = database.query(
        """
        SELECT employer_name, normalized_name, city, state,
               initial_approvals, continuing_approvals, initial_denials,
               fiscal_year
        FROM h1b_sponsors
        WHERE normalized_name IS NOT NULL AND normalized_name != ''
    """
    )

    grouped = {}
    for row in rows:
        key = row["normalized_name"]
        if key not in grouped:
            grouped[key] = {
                "employer_name": row["employer_name"],
                "normalized_name": key,
                "initial_approvals": row["initial_approvals"] or 0,
                "continuing_approvals": row["continuing_approvals"] or 0,
                "initial_denials": row["initial_denials"] or 0,
                "fiscal_year": row["fiscal_year"],
            }
        else:
            existing = grouped[key]
            existing["initial_approvals"] += row["initial_approvals"] or 0
            existing["continuing_approvals"] += row["continuing_approvals"] or 0
            existing["initial_denials"] += row["initial_denials"] or 0
            if (row.get("fiscal_year") or "") > (existing.get("fiscal_year") or ""):
                existing["fiscal_year"] = row["fiscal_year"]

    return grouped


def _fuzzy_match_names(
    sec_names: list[str],
    h1b_names: list[str],
    threshold: int = config.FUZZY_MATCH_THRESHOLD,
) -> dict[str, str]:
    """Find fuzzy matches between SEC and H1B normalized names.

    Strategy for speed:
      1. Exact match (O(1) set lookup)
      2. Prefix-bucketed fuzzy match — only compare names sharing the
         same 2-character prefix, cutting comparison space by ~300x
      3. Length-filtered — skip pairs where length differs by >50%

    Args:
        sec_names: List of normalized SEC company names.
        h1b_names: List of normalized H1B employer names.
        threshold: Minimum fuzzy match score (0-100).

    Returns:
        Dict mapping SEC normalized_name -> matching H1B normalized_name.
    """
    h1b_set = set(h1b_names)
    matches = {}

    # ── Pass 1: Exact matches ────────────────────────────────────────────
    for sec_name in sec_names:
        if sec_name in h1b_set:
            matches[sec_name] = sec_name

    exact_count = len(matches)
    print(f"  Exact matches: {exact_count}")

    # ── Pass 2: Prefix-bucketed fuzzy matching ───────────────────────────
    unmatched_sec = [n for n in sec_names if n not in matches]
    matched_h1b_values = set(matches.values())
    unmatched_h1b = [n for n in h1b_names if n not in matched_h1b_values]

    if not unmatched_sec or not unmatched_h1b:
        return matches

    print(
        f"  Fuzzy matching {len(unmatched_sec)} SEC names against "
        f"{len(unmatched_h1b)} H1B names (prefix-bucketed)..."
    )

    # Build prefix buckets for H1B names (2-char prefix)
    PREFIX_LEN = 2
    h1b_buckets = defaultdict(list)
    for name in unmatched_h1b:
        if len(name) >= PREFIX_LEN:
            h1b_buckets[name[:PREFIX_LEN]].append(name)

    # Also build 3-char buckets for fallback on large 2-char buckets
    h1b_buckets_3 = defaultdict(list)
    for name in unmatched_h1b:
        if len(name) >= 3:
            h1b_buckets_3[name[:3]].append(name)

    fuzzy_count = 0
    processed = 0

    for sec_name in unmatched_sec:
        if len(sec_name) < PREFIX_LEN:
            continue

        prefix = sec_name[:PREFIX_LEN]
        candidates = h1b_buckets.get(prefix, [])

        # If bucket is very large (>5000), use 3-char prefix for tighter filter
        if len(candidates) > 5000 and len(sec_name) >= 3:
            candidates = h1b_buckets_3.get(sec_name[:3], [])

        if not candidates:
            processed += 1
            continue

        # Length filter: skip candidates where length differs by more than 50%
        sec_len = len(sec_name)
        min_len = int(sec_len * 0.5)
        max_len = int(sec_len * 1.5) + 1
        filtered = [c for c in candidates if min_len <= len(c) <= max_len]

        if not filtered:
            processed += 1
            continue

        # Use rapidfuzz extractOne (C++ optimized with early abort)
        result = process.extractOne(
            sec_name,
            filtered,
            scorer=fuzz.ratio,
            score_cutoff=threshold,
        )

        if result is not None:
            match_name, score, _idx = result
            matches[sec_name] = match_name
            fuzzy_count += 1

        processed += 1
        if processed % 20000 == 0:
            print(
                f"    Processed {processed}/{len(unmatched_sec)} "
                f"({fuzzy_count} fuzzy matches so far)",
                flush=True,
            )

    print(
        f"    Processed {processed}/{len(unmatched_sec)} "
        f"({fuzzy_count} fuzzy matches total)"
    )
    print(f"  Fuzzy matches: {fuzzy_count}")
    return matches


def build_matched_companies():
    """Build the matched_companies table by cross-referencing SEC and H1B data.

    Uses upsert (INSERT ... ON CONFLICT DO UPDATE) keyed on normalized_name
    to preserve existing row IDs.  This keeps foreign-key references in
    job_listings and company_ats_status intact across re-runs.

    1. Exact-match on normalized names
    2. Fuzzy-match remaining names with threshold (prefix-bucketed)
    3. Tag source as 'both', 'sec_only', or 'h1b_only'
    4. Upsert into matched_companies table (preserving IDs)
    """
    database.init_db()
    # NOTE: we do NOT clear_table — upsert preserves existing row IDs so
    # that foreign keys in job_listings / company_ats_status remain valid.

    print("Building matched company list...")

    sec_companies = _get_unique_sec_companies()
    h1b_sponsors = _get_unique_h1b_sponsors()

    print(f"  SEC companies: {len(sec_companies)}")
    print(f"  H1B sponsors:  {len(h1b_sponsors)}")

    # Find matches
    matches = _fuzzy_match_names(
        list(sec_companies.keys()),
        list(h1b_sponsors.keys()),
    )

    print(f"  Matched pairs (exact + fuzzy): {len(matches)}")

    # Build records
    records = []
    seen_normalized = set()

    # 1. Companies in BOTH sources
    for sec_name, h1b_name in matches.items():
        sec = sec_companies[sec_name]
        h1b = h1b_sponsors[h1b_name]

        company_name = h1b.get("employer_name") or sec.get("company_name", "")
        total_approvals = h1b.get("initial_approvals", 0) + h1b.get(
            "continuing_approvals", 0
        )

        records.append(
            {
                "company_name": company_name,
                "normalized_name": sec_name,
                "source": "both",
                "h1b_approval_count": total_approvals,
                "sec_amount_raised": sec.get("total_amount_sold", 0),
                "priority_score": 0,
            }
        )
        seen_normalized.add(sec_name)
        seen_normalized.add(h1b_name)

    # 2. SEC-only companies
    for name, sec in sec_companies.items():
        if name not in seen_normalized:
            records.append(
                {
                    "company_name": sec["company_name"],
                    "normalized_name": name,
                    "source": "sec_only",
                    "h1b_approval_count": 0,
                    "sec_amount_raised": sec.get("total_amount_sold", 0),
                    "priority_score": 0,
                }
            )
            seen_normalized.add(name)

    # 3. H1B-only sponsors
    for name, h1b in h1b_sponsors.items():
        if name not in seen_normalized:
            total_approvals = h1b.get("initial_approvals", 0) + h1b.get(
                "continuing_approvals", 0
            )
            records.append(
                {
                    "company_name": h1b["employer_name"],
                    "normalized_name": name,
                    "source": "h1b_only",
                    "h1b_approval_count": total_approvals,
                    "sec_amount_raised": 0,
                    "priority_score": 0,
                }
            )
            seen_normalized.add(name)

    # Upsert into database (preserves existing IDs)
    if records:
        _upsert_matched_companies(records)

    both_count = sum(1 for r in records if r["source"] == "both")
    sec_only = sum(1 for r in records if r["source"] == "sec_only")
    h1b_only = sum(1 for r in records if r["source"] == "h1b_only")

    print(f"\nMatched companies summary:")
    print(f"  Both SEC + H1B: {both_count}")
    print(f"  SEC only:       {sec_only}")
    print(f"  H1B only:       {h1b_only}")
    print(f"  Total:          {len(records)}")

    return len(records)


def _upsert_matched_companies(records: list[dict]):
    """Upsert matched companies, preserving existing row IDs.

    Uses ON CONFLICT(normalized_name) so that companies already in the table
    keep their primary key, and foreign-key references from job_listings and
    company_ats_status remain intact.
    """
    sql = """
        INSERT INTO matched_companies
            (company_name, normalized_name, source,
             h1b_approval_count, sec_amount_raised, priority_score)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(normalized_name) DO UPDATE SET
            company_name = excluded.company_name,
            source = excluded.source,
            h1b_approval_count = excluded.h1b_approval_count,
            sec_amount_raised = excluded.sec_amount_raised,
            priority_score = excluded.priority_score
    """
    values = [
        (
            r["company_name"],
            r["normalized_name"],
            r["source"],
            r["h1b_approval_count"],
            r["sec_amount_raised"],
            r["priority_score"],
        )
        for r in records
    ]
    with database.get_db() as conn:
        conn.executemany(sql, values)
