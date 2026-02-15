#!/usr/bin/env python3
"""Discover Workday career page URLs for H1B sponsor companies.

Strategy:
  1. Seed list — cross-reference a known list of ~250 Workday tenants
     (from killerfrost598/Workday-Scraper robots.json) against all our
     matched companies that don't already use Greenhouse/Lever/Ashby.
     Deduplicates per tenant, keeping the best-matching company.
  2. Live probe — for remaining companies, generate tenant-name candidates
     and check robots.txt to discover the board name.

Usage:
  python -m collectors.workday_urls                   # default: seed + probe top 1000
  python -m collectors.workday_urls --limit 2000      # probe more companies
  python -m collectors.workday_urls --seed-only       # fast: seed list only
  python -m collectors.workday_urls --probe-only      # skip seed list
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ── Constants ────────────────────────────────────────────────────────────────

WORKDAY_SUBDOMAINS = ["wd5", "wd1", "wd3", "wd2"]

SEED_LIST_URL = (
    "https://raw.githubusercontent.com/killerfrost598/"
    "Workday-Scraper/main/robots.json"
)

_CORP_RE = re.compile(
    r"\b(inc|llc|ltd|lp|llp|corp|corporation|co|company|group|holdings|"
    r"technologies|technology|tech|solutions|systems|services|consulting|"
    r"software|labs|international|global|americas|usa|us|plc|sa|gmbh|ag)\b",
    re.IGNORECASE,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _strip_corp(name: str) -> str:
    """Lowercase and strip corporate suffixes from a company name."""
    s = _CORP_RE.sub("", name.lower()).strip()
    return re.sub(r"\s+", " ", s).strip()


def _slug(s: str) -> str:
    """Lowercase alphanumeric slug."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _pick_best_board(boards: list[str]) -> str:
    """Pick the most useful external career board from a list."""
    for b in boards:
        bl = b.lower()
        if "external" in bl or "career" in bl or "jobs" in bl:
            return b
    return boards[0] if boards else ""


def _generate_tenant_candidates(company_name: str, normalized: str) -> list[tuple[str, float]]:
    """Generate (candidate, quality_weight) pairs for Workday tenant matching."""
    seen: set[str] = set()
    candidates: list[tuple[str, float]] = []

    def _add(slug: str, weight: float):
        slug = slug.strip().strip("-")
        if slug and slug not in seen and len(slug) >= 3:
            seen.add(slug)
            candidates.append((slug, weight))

    stripped = _strip_corp(company_name)
    stripped_slug = _slug(stripped)

    _add(normalized, 1.0)
    _add(stripped_slug, 0.95)

    # Hyphenated version
    hyph = re.sub(r"\s+", "-", stripped).strip("-")
    hyph = re.sub(r"[^a-z0-9-]", "", hyph).strip("-")
    _add(hyph, 0.9)

    # Full name with suffixes
    _add(_slug(company_name), 0.8)

    # First word (only if significant portion of name)
    words = company_name.split()
    if words:
        first = _slug(words[0])
        ratio = len(first) / max(len(stripped_slug), 1)
        if ratio > 0.4:
            _add(first, ratio)

    # First two words
    if len(words) >= 2:
        _add(_slug(words[0] + words[1]), 0.85)

    return candidates


# ── Seed list ────────────────────────────────────────────────────────────────


def load_seed_list() -> dict[str, dict]:
    """Download and parse the known Workday tenants seed list."""
    print("  Downloading seed list ...")
    try:
        resp = requests.get(SEED_LIST_URL, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [warn] Could not download seed list: {e}")
        return {}

    tenants: dict[str, dict] = {}
    for line in resp.text.strip().splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line.strip())
        except json.JSONDecodeError:
            continue

        name = obj.get("Company_name", "").strip().lower()
        sitemaps = obj.get("Sitemap", [])
        allows = obj.get("Allow", [])

        if not name or not sitemaps:
            continue

        m = re.search(r"https?://([^.]+)\.(wd\d+)\.myworkdayjobs\.com", sitemaps[0])
        if not m:
            continue

        boards = [b.strip("/") for b in allows if b.strip("/") != "refreshFacet"]
        tenants[name] = {
            "tenant": m.group(1),
            "subdomain": m.group(2),
            "boards": boards,
        }

    print(f"  Loaded {len(tenants)} known Workday tenants")
    return tenants


def match_seed_list(
    companies: list[dict], seed: dict[str, dict]
) -> tuple[list[dict], list[dict]]:
    """Match companies against the seed list.

    Returns:
        (results, remaining) — results deduplicated per Workday tenant
        (best-matching company wins), remaining are companies that didn't match.
    """
    # For each seed tenant, track the best matching company
    tenant_best: dict[str, tuple[float, dict, float]] = {}  # tenant -> (score, company, similarity)
    unmatched: list[dict] = []

    for company in companies:
        name = company["company_name"]
        norm = company["normalized_name"]
        stripped_slug = _slug(_strip_corp(name))
        candidates = _generate_tenant_candidates(name, norm)

        matched = False
        for cand, quality in candidates:
            if cand in seed:
                # Compute similarity between candidate and stripped company name
                if stripped_slug and cand:
                    if cand == stripped_slug:
                        sim = 1.0
                    elif stripped_slug.startswith(cand) or cand.startswith(stripped_slug):
                        sim = min(len(cand), len(stripped_slug)) / max(len(cand), len(stripped_slug))
                    else:
                        sim = 0
                else:
                    sim = 0

                if sim < 0.3:
                    continue

                score = quality * sim * 100 + company["priority_score"]
                if cand not in tenant_best or score > tenant_best[cand][0]:
                    tenant_best[cand] = (score, company, sim)
                matched = True
                break

        if not matched:
            unmatched.append(company)

    results = []
    for tenant_key, (_, company, sim) in tenant_best.items():
        info = seed[tenant_key]
        board = _pick_best_board(info["boards"])
        url = (
            f"https://{info['tenant']}.{info['subdomain']}"
            f".myworkdayjobs.com/{board}"
        )
        results.append({
            "company_name": company["company_name"],
            "normalized_name": company["normalized_name"],
            "priority_score": company["priority_score"],
            "tenant": info["tenant"],
            "subdomain": info["subdomain"],
            "board": board,
            "all_boards": ", ".join(info["boards"]),
            "url": url,
            "method": "seed",
            "match_quality": f"{sim:.0%}",
        })

    return results, unmatched


# ── Live probing ─────────────────────────────────────────────────────────────


def _probe_robots_txt(tenant: str, subdomain: str, timeout: float = 4.0) -> list[str] | None:
    """Fetch robots.txt for a Workday tenant. Returns board names or None."""
    url = f"https://{tenant}.{subdomain}.myworkdayjobs.com/robots.txt"
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        boards = re.findall(r"Allow:\s*/([^/\s]+)/", r.text)
        return boards if boards else None
    except Exception:
        return None


def probe_company(company_name: str, normalized: str) -> dict | None:
    """Try to discover a Workday career page via robots.txt probing."""
    candidates = _generate_tenant_candidates(company_name, normalized)

    for cand, _ in candidates:
        for sub in WORKDAY_SUBDOMAINS:
            boards = _probe_robots_txt(cand, sub)
            if boards:
                board = _pick_best_board(boards)
                return {
                    "tenant": cand,
                    "subdomain": sub,
                    "boards": boards,
                    "board": board,
                    "url": f"https://{cand}.{sub}.myworkdayjobs.com/{board}",
                }
    return None


# ── Database ─────────────────────────────────────────────────────────────────


def _get_companies_without_ats(limit: int | None = None) -> list[dict]:
    """Get companies that don't use Greenhouse/Lever/Ashby."""
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from db import database

    database.init_db()

    sql = """
        SELECT mc.id, mc.company_name, mc.normalized_name, mc.priority_score
        FROM matched_companies mc
        LEFT JOIN company_ats_status cas
            ON cas.normalized_name = mc.normalized_name
        WHERE cas.ats_system IS NULL OR cas.id IS NULL
        ORDER BY mc.priority_score DESC
    """
    params: tuple = ()
    if limit:
        sql += " LIMIT ?"
        params = (limit,)

    return database.query(sql, params)


# ── Main ─────────────────────────────────────────────────────────────────────


def run_discovery(
    limit: int | None = None,
    probe_limit: int = 1000,
    workers: int = 8,
    skip_seed: bool = False,
    skip_probe: bool = False,
) -> list[dict]:
    """Discover Workday URLs for companies without a known ATS.

    Args:
        limit: If set, only consider top N companies by priority.
               If None, use ALL companies for seed matching.
        probe_limit: Max companies to live-probe (after seed matching).
        workers: Threads for live probing.
        skip_seed: Skip the seed list stage.
        skip_probe: Skip the live probing stage.
    """
    # For seed matching, use all companies (fast, in-memory).
    # For probing, respect the limit (slow, network I/O).
    all_companies = _get_companies_without_ats(limit=limit)
    total = len(all_companies)

    if not all_companies:
        print("No companies without a known ATS.")
        return []

    print(f"\nDiscovering Workday URLs for {total:,} companies "
          f"(excluding GH/Lever/Ashby) ...\n")

    results: list[dict] = []
    remaining: list[dict] = list(all_companies)

    # ── Stage 1: Seed list ───────────────────────────────────────────────
    if not skip_seed:
        seed = load_seed_list()
        if seed:
            print(f"\nStage 1: Matching against {len(seed)} known tenants ...")
            seed_results, remaining = match_seed_list(all_companies, seed)
            results.extend(seed_results)
            print(f"  Found: {len(seed_results)} companies")
            print(f"  Remaining: {len(remaining):,}\n")

    # ── Stage 2: Live probing ────────────────────────────────────────────
    if not skip_probe and remaining:
        to_probe = remaining[:probe_limit]
        print(f"Stage 2: Live robots.txt probing for {len(to_probe):,} companies "
              f"({workers} workers) ...")
        probe_found = 0
        completed = 0
        seen_tenants = {r["tenant"] for r in results}

        def _do_probe(company):
            return company, probe_company(
                company["company_name"], company["normalized_name"]
            )

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_do_probe, c): c for c in to_probe}
            for future in as_completed(futures):
                completed += 1
                company, result = future.result()

                if result and result["tenant"] not in seen_tenants:
                    probe_found += 1
                    seen_tenants.add(result["tenant"])
                    results.append({
                        "company_name": company["company_name"],
                        "normalized_name": company["normalized_name"],
                        "priority_score": company["priority_score"],
                        "tenant": result["tenant"],
                        "subdomain": result["subdomain"],
                        "board": result["board"],
                        "all_boards": ", ".join(result["boards"]),
                        "url": result["url"],
                        "method": "probe",
                        "match_quality": "100%",
                    })
                    name = company["company_name"][:45]
                    sys.stdout.write(
                        f"  [{completed}/{len(to_probe)}] {name:<45} -> {result['url']}\n"
                    )
                    sys.stdout.flush()
                else:
                    if completed % 100 == 0 or completed == len(to_probe):
                        sys.stdout.write(f"  [{completed}/{len(to_probe)}] probing ...\n")
                        sys.stdout.flush()

        print(f"\n  Probe results: {probe_found} additional\n")

    # ── Summary ──────────────────────────────────────────────────────────
    results.sort(key=lambda r: r.get("priority_score", 0), reverse=True)

    seed_count = sum(1 for r in results if r["method"] == "seed")
    probe_count = sum(1 for r in results if r["method"] == "probe")

    print("=" * 70)
    print(f"Discovery complete: {len(results)} companies with Workday career pages")
    print(f"  From seed list:  {seed_count}")
    print(f"  From probing:    {probe_count}")
    print("=" * 70)

    return results


def save_csv(results: list[dict], output_path: str):
    """Write results to a CSV file."""
    if not results:
        print("No results to save.")
        return

    fieldnames = [
        "company_name", "normalized_name", "priority_score",
        "url", "tenant", "subdomain", "board", "all_boards",
        "method", "match_quality",
    ]
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved {len(results)} results to {output_path}")


# ── CLI ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Discover Workday career page URLs for H1B sponsors "
                    "(excluding GH/Lever/Ashby companies).",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Only consider top N companies by priority (default: all)",
    )
    parser.add_argument(
        "--probe-limit", type=int, default=1000,
        help="Max companies to live-probe after seed matching (default: 1000)",
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=8,
        help="Concurrent threads for probing (default: 8)",
    )
    parser.add_argument(
        "-o", "--output", type=str, default=None,
        help="Output CSV path (default: output/workday_urls.csv)",
    )
    parser.add_argument(
        "--seed-only", action="store_true",
        help="Only match against seed list (no live probing)",
    )
    parser.add_argument(
        "--probe-only", action="store_true",
        help="Only do live probing (skip seed list)",
    )

    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = args.output or os.path.join(base_dir, "output", "workday_urls.csv")

    results = run_discovery(
        limit=args.limit,
        probe_limit=args.probe_limit,
        workers=args.workers,
        skip_seed=args.probe_only,
        skip_probe=args.seed_only,
    )

    save_csv(results, output_path)

    if results:
        print(f"\nTop 25 companies with Workday pages:")
        print(f"{'Score':>6}  {'Company':<45} URL")
        print("-" * 120)
        for r in results[:25]:
            print(f"{r['priority_score']:>6.1f}  {r['company_name'][:44]:<45} {r['url']}")


def import_to_db(csv_path: str | None = None) -> int:
    """Import workday_urls.csv into the workday_boards table and set ATS status.

    Reads the CSV, matches each row to a company in matched_companies,
    upserts into workday_boards, and marks each company as ats_system='workday'
    in company_ats_status.

    Args:
        csv_path: Path to workday_urls.csv. Defaults to config.WORKDAY_URLS_CSV.

    Returns:
        Number of rows imported.
    """
    from db import database
    import config as cfg

    database.init_db()

    if csv_path is None:
        csv_path = cfg.WORKDAY_URLS_CSV

    if not os.path.exists(csv_path):
        print(f"  Workday CSV not found: {csv_path}")
        return 0

    # Read CSV
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("tenant") and row.get("board"):
                rows.append(row)

    if not rows:
        print("  No valid rows in workday CSV.")
        return 0

    print(f"  Importing {len(rows)} Workday boards from {csv_path}...")

    # Build a lookup of matched_companies by normalized_name
    companies = database.query(
        "SELECT id, company_name, normalized_name FROM matched_companies"
    )
    by_norm = {c["normalized_name"]: c for c in companies}

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    imported = 0

    with database.get_db() as conn:
        for row in rows:
            tenant = row["tenant"]
            subdomain = row.get("subdomain", "wd5")
            board = row["board"]
            url = row.get("url", f"https://{tenant}.{subdomain}.myworkdayjobs.com/{board}")
            job_count = int(row.get("job_count", 0) or 0)
            norm = row.get("normalized_name", tenant)

            # Resolve company_id
            mc = by_norm.get(norm)
            company_id = mc["id"] if mc else None
            company_name = row.get("company_name", tenant)

            # Upsert into workday_boards
            conn.execute(
                """INSERT INTO workday_boards
                       (company_id, normalized_name, tenant, subdomain, board, url, job_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(tenant, board) DO UPDATE SET
                       company_id = excluded.company_id,
                       normalized_name = excluded.normalized_name,
                       url = excluded.url,
                       job_count = excluded.job_count""",
                (company_id, norm, tenant, subdomain, board, url, job_count),
            )

            # Set company_ats_status = 'workday'
            if company_id:
                conn.execute(
                    """INSERT INTO company_ats_status
                           (company_id, normalized_name, ats_system, last_checked, has_jobs)
                       VALUES (?, ?, 'workday', ?, ?)
                       ON CONFLICT(normalized_name) DO UPDATE SET
                           ats_system = 'workday',
                           last_checked = excluded.last_checked,
                           has_jobs = excluded.has_jobs""",
                    (company_id, norm, now, 1 if job_count > 0 else 0),
                )

            imported += 1

    print(f"  Imported {imported} Workday boards into database.")
    return imported


if __name__ == "__main__":
    main()
