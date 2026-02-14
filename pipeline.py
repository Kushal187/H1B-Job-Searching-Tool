#!/usr/bin/env python3
"""H1B Job Search Tool — Main Pipeline Orchestrator.

CLI with subcommands for each phase of the pipeline:
  collect   — Phase 1: Download and parse SEC Form D + H1B/LCA data
  match     — Phase 2: Normalize names, fuzzy-match, and score companies
  scrape    — Phase 3: Scrape Greenhouse and Lever for open jobs
  export    — Export results to CSV and JSON
  run-all   — Run all phases end-to-end

Scraping modes:
  discovery — First run: check ALL companies, cache which ATS they use
  monitor   — Daily run: only re-check companies known to use Greenhouse/Lever

Usage:
  python pipeline.py collect
  python pipeline.py match
  python pipeline.py scrape --mode discovery --workers 10
  python pipeline.py scrape --mode monitor
  python pipeline.py export
  python pipeline.py run-all
"""

import argparse
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import config
from db import database


# ─── Phase 1: Data Collection ───────────────────────────────────────────────


def cmd_collect(args):
    """Download and parse all data sources into the database."""
    print("=" * 60)
    print("PHASE 1: Data Collection")
    print("=" * 60)

    database.init_db()

    # SEC Form D
    print("\n--- SEC Form D Data ---")
    from collectors.sec_formd import load_to_db as load_sec
    sec_count = load_sec()

    # H1B / LCA
    print("\n--- H1B / LCA Employer Data ---")
    from collectors.h1b_data import load_to_db as load_h1b
    h1b_count = load_h1b()

    print("\n" + "=" * 60)
    print(f"Collection complete: {sec_count} SEC records, {h1b_count} H1B records")
    print("=" * 60)


# ─── Phase 2: Matching ──────────────────────────────────────────────────────


def cmd_match(args):
    """Normalize, fuzzy-match, and score companies."""
    print("=" * 60)
    print("PHASE 2: Company Matching & Scoring")
    print("=" * 60)

    database.init_db()

    # Build matches
    print("\n--- Fuzzy Matching ---")
    from matching.matcher import build_matched_companies
    match_count = build_matched_companies()

    # Score companies
    print("\n--- Priority Scoring ---")
    from matching.scorer import update_priority_scores
    update_priority_scores()

    print("\n" + "=" * 60)
    print(f"Matching complete: {match_count} companies in matched list")
    print("=" * 60)


# ─── Phase 3: Job Scraping ───────────────────────────────────────────────────


def _scrape_one_company(company: dict, gh_dir: str, lever_dir: str) -> dict:
    """Scrape a single company (Greenhouse then Lever). Thread-safe.

    Returns a result dict with keys:
      name, normalized, ats, job_count, new_job_count, status
    """
    from scrapers.greenhouse import scrape_greenhouse
    from scrapers.lever import scrape_lever

    name = company["company_name"]
    norm = company["normalized_name"]
    cid = company["id"]

    result = {
        "name": name,
        "normalized": norm,
        "company_id": cid,
        "ats": None,
        "job_count": 0,
        "new_job_count": 0,
        "total_before_filter": 0,
        "status": "not_found",
    }

    # Try Greenhouse first
    gh = scrape_greenhouse(name, norm, gh_dir, company_id=cid)
    if gh is not None:
        result["ats"] = "greenhouse"
        result["job_count"] = gh.get("job_count", 0)
        result["new_job_count"] = gh.get("new_job_count", 0)
        result["total_before_filter"] = gh.get("total_before_filter", 0)
        result["status"] = "found" if gh.get("job_count", 0) > 0 else "ats_no_match"
        return result

    # Then try Lever
    lv = scrape_lever(name, norm, lever_dir, company_id=cid)
    if lv is not None:
        result["ats"] = "lever"
        result["job_count"] = lv.get("job_count", 0)
        result["new_job_count"] = lv.get("new_job_count", 0)
        result["total_before_filter"] = lv.get("total_before_filter", 0)
        result["status"] = "found" if lv.get("job_count", 0) > 0 else "ats_no_match"
        return result

    return result


def _update_ats_status(result: dict):
    """Cache the ATS status for a company in company_ats_status table."""
    now = datetime.now(timezone.utc).isoformat()
    has_jobs = 1 if result["job_count"] > 0 else 0

    if result["ats"]:
        database.execute(
            """INSERT INTO company_ats_status (company_id, normalized_name, ats_system, last_checked, has_jobs)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(normalized_name) DO UPDATE SET
                   ats_system = excluded.ats_system,
                   last_checked = excluded.last_checked,
                   has_jobs = excluded.has_jobs""",
            (result["company_id"], result["normalized"], result["ats"], now, has_jobs),
        )
    else:
        database.execute(
            """INSERT INTO company_ats_status (company_id, normalized_name, ats_system, last_checked, has_jobs)
               VALUES (?, ?, NULL, ?, 0)
               ON CONFLICT(normalized_name) DO UPDATE SET
                   last_checked = excluded.last_checked,
                   has_jobs = 0""",
            (result["company_id"], result["normalized"], now),
        )


def cmd_scrape(args):
    """Scrape Greenhouse and Lever for open jobs at matched companies."""
    mode = getattr(args, "mode", "monitor")
    workers = getattr(args, "workers", 1)
    limit = getattr(args, "limit", None)

    print("=" * 60)
    print(f"PHASE 3: Job Scraping (mode={mode}, workers={workers})")
    print("=" * 60)

    database.init_db()

    # Do NOT clear job_listings — we use upsert to preserve history

    from scrapers.greenhouse import scrape_greenhouse
    from scrapers.lever import scrape_lever

    if mode == "discovery":
        # Scrape ALL companies (or up to --limit)
        sql = (
            "SELECT id, company_name, normalized_name, priority_score "
            "FROM matched_companies ORDER BY priority_score DESC"
        )
        params = ()
        if limit:
            sql += " LIMIT ?"
            params = (limit,)
        companies = database.query(sql, params)

    elif mode == "monitor":
        # Only scrape companies previously found on Greenhouse or Lever
        companies = database.query(
            """SELECT mc.id, mc.company_name, mc.normalized_name, mc.priority_score
               FROM matched_companies mc
               INNER JOIN company_ats_status cas ON cas.normalized_name = mc.normalized_name
               WHERE cas.ats_system IS NOT NULL
               ORDER BY mc.priority_score DESC""",
        )
        if not companies:
            print("\nNo companies with known ATS found.")
            print("Run with --mode discovery first to identify which companies use Greenhouse/Lever.")
            return

    else:
        print(f"Unknown mode: {mode}")
        return

    if not companies:
        print("No matched companies found. Run 'match' first.")
        return

    total = len(companies)
    print(f"\nScraping {total:,} companies with {workers} worker(s)...\n")

    os.makedirs(config.GREENHOUSE_DIR, exist_ok=True)
    os.makedirs(config.LEVER_DIR, exist_ok=True)

    # Counters
    stats = {
        "greenhouse": 0,
        "lever": 0,
        "ats_no_match": 0,  # company uses ATS but no matching jobs
        "not_found": 0,
        "total_jobs": 0,
        "new_jobs": 0,
    }

    start_time = time.time()

    if workers <= 1:
        # Sequential scraping
        for i, company in enumerate(companies):
            result = _scrape_one_company(
                company, config.GREENHOUSE_DIR, config.LEVER_DIR,
            )
            _update_ats_status(result)
            _print_progress(i + 1, total, result, start_time)
            _tally_stats(stats, result)
    else:
        # Concurrent scraping with ThreadPoolExecutor
        completed = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _scrape_one_company, company, config.GREENHOUSE_DIR, config.LEVER_DIR,
                ): company
                for company in companies
            }

            for future in as_completed(futures):
                completed += 1
                try:
                    result = future.result()
                    _update_ats_status(result)
                    _print_progress(completed, total, result, start_time)
                    _tally_stats(stats, result)
                except Exception as e:
                    company = futures[future]
                    print(f"  [{completed}/{total}] {company['company_name']} -> ERROR: {e}")
                    stats["not_found"] += 1

    elapsed = time.time() - start_time
    _print_summary(stats, total, elapsed)


def _print_progress(i: int, total: int, result: dict, start_time: float):
    """Print a single-line progress update."""
    elapsed = time.time() - start_time
    rate = i / elapsed if elapsed > 0 else 0
    eta_seconds = (total - i) / rate if rate > 0 else 0
    eta_min = int(eta_seconds // 60)
    eta_sec = int(eta_seconds % 60)

    name = result["name"][:40]
    if result["status"] == "found":
        new_tag = f" ({result['new_job_count']} new)" if result.get("new_job_count") else ""
        print(
            f"  [{i:,}/{total:,}] {name:<40} "
            f"-> {result['ats'].upper()} ({result['job_count']} jobs{new_tag}) "
            f"[ETA {eta_min}m{eta_sec:02d}s]"
        )
    elif result["status"] == "ats_no_match":
        print(
            f"  [{i:,}/{total:,}] {name:<40} "
            f"-> {result['ats'].upper()} ({result['total_before_filter']} jobs, 0 after filter) "
            f"[ETA {eta_min}m{eta_sec:02d}s]"
        )
    else:
        # Only print not-found in discovery mode (too noisy otherwise)
        if i % 100 == 0 or i == total:
            print(
                f"  [{i:,}/{total:,}] ... "
                f"[ETA {eta_min}m{eta_sec:02d}s, {rate:.1f} companies/sec]"
            )


def _tally_stats(stats: dict, result: dict):
    """Accumulate scraping statistics."""
    if result["status"] == "found":
        if result["ats"] == "greenhouse":
            stats["greenhouse"] += 1
        else:
            stats["lever"] += 1
        stats["total_jobs"] += result["job_count"]
        stats["new_jobs"] += result.get("new_job_count", 0)
    elif result["status"] == "ats_no_match":
        stats["ats_no_match"] += 1
    else:
        stats["not_found"] += 1


def _print_summary(stats: dict, total: int, elapsed: float):
    """Print scraping summary."""
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    found = stats["greenhouse"] + stats["lever"]
    print(f"\n{'=' * 60}")
    print(f"Scraping complete in {minutes}m {seconds}s")
    print(f"{'=' * 60}")
    print(f"  Companies scraped:   {total:,}")
    print(f"  Greenhouse matches:  {stats['greenhouse']:,}")
    print(f"  Lever matches:       {stats['lever']:,}")
    print(f"  ATS found, 0 match:  {stats['ats_no_match']:,}")
    print(f"  Not on either ATS:   {stats['not_found']:,}")
    print(f"  Match rate:          {found / total * 100:.1f}%" if total > 0 else "  N/A")
    print(f"  Total matching jobs: {stats['total_jobs']:,}")
    print(f"  New jobs this run:   {stats['new_jobs']:,}")
    print("=" * 60)


# ─── Export ──────────────────────────────────────────────────────────────────


def cmd_export(args):
    """Export results to CSV and JSON files."""
    print("=" * 60)
    print("EXPORT: Generating Reports")
    print("=" * 60)

    database.init_db()
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    # 1. matched_companies.csv
    _export_matched_companies()

    # 2. companies_with_jobs.csv
    _export_companies_with_jobs()

    # 3. new_jobs.csv — jobs discovered in the last 24 hours
    _export_new_jobs()

    # 4. summary_report.json
    _export_summary_report()

    print(f"\nAll exports saved to {config.OUTPUT_DIR}/")


def _export_matched_companies():
    """Export all matched companies ranked by priority score."""
    path = os.path.join(config.OUTPUT_DIR, "matched_companies.csv")
    rows = database.query(
        "SELECT company_name, normalized_name, source, h1b_approval_count, "
        "sec_amount_raised, priority_score FROM matched_companies "
        "ORDER BY priority_score DESC"
    )

    if not rows:
        print("  No matched companies to export.")
        return

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"  matched_companies.csv: {len(rows):,} companies")


def _export_companies_with_jobs():
    """Export companies that have open jobs on Greenhouse or Lever."""
    path = os.path.join(config.OUTPUT_DIR, "companies_with_jobs.csv")
    rows = database.query("""
        SELECT DISTINCT
            mc.company_name,
            mc.normalized_name,
            mc.source,
            mc.h1b_approval_count,
            mc.priority_score,
            jl.ats_system,
            COUNT(jl.id) as job_count
        FROM matched_companies mc
        INNER JOIN job_listings jl ON jl.company_id = mc.id
        GROUP BY mc.id, jl.ats_system
        ORDER BY mc.priority_score DESC
    """)

    if not rows:
        print("  No companies with jobs to export. Run 'scrape' first.")
        return

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"  companies_with_jobs.csv: {len(rows):,} entries")


def _export_new_jobs():
    """Export jobs posted in the last 7 days (by company's publish date)."""
    path = os.path.join(config.OUTPUT_DIR, "new_jobs.csv")
    rows = database.query("""
        SELECT
            jl.company_name,
            jl.ats_system,
            jl.job_title,
            jl.job_location,
            jl.job_url,
            jl.department,
            jl.posted_at,
            jl.first_seen_at,
            mc.h1b_approval_count,
            mc.priority_score
        FROM job_listings jl
        LEFT JOIN matched_companies mc ON mc.id = jl.company_id
        WHERE jl.posted_at >= datetime('now', '-7 days')
        ORDER BY jl.posted_at DESC
    """)

    if not rows:
        print("  new_jobs.csv: 0 jobs posted in the last 7 days")
        return

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"  new_jobs.csv: {len(rows):,} jobs posted in last 7 days")


def _export_summary_report():
    """Export aggregate stats as a JSON report."""
    path = os.path.join(config.OUTPUT_DIR, "summary_report.json")

    # Gather stats
    sec_count = database.query("SELECT COUNT(*) as cnt FROM sec_formd_companies")[0]["cnt"]
    h1b_count = database.query("SELECT COUNT(*) as cnt FROM h1b_sponsors")[0]["cnt"]
    matched_count = database.query("SELECT COUNT(*) as cnt FROM matched_companies")[0]["cnt"]
    job_count = database.query("SELECT COUNT(*) as cnt FROM job_listings")[0]["cnt"]

    # ATS discovery stats
    ats_stats = database.query("""
        SELECT
            COALESCE(ats_system, 'not_found') as ats,
            COUNT(*) as companies,
            SUM(has_jobs) as with_matching_jobs
        FROM company_ats_status
        GROUP BY ats_system
    """)

    source_breakdown = database.query(
        "SELECT source, COUNT(*) as cnt FROM matched_companies GROUP BY source"
    )

    ats_breakdown = database.query(
        "SELECT ats_system, COUNT(DISTINCT company_id) as companies, COUNT(*) as jobs "
        "FROM job_listings GROUP BY ats_system"
    )

    # New jobs in last 24h (by actual posting date)
    new_24h = database.query(
        "SELECT COUNT(*) as cnt FROM job_listings "
        "WHERE posted_at >= datetime('now', '-24 hours')"
    )[0]["cnt"]

    top_companies = database.query(
        "SELECT mc.company_name, mc.priority_score, mc.h1b_approval_count, "
        "COUNT(jl.id) as open_jobs "
        "FROM matched_companies mc "
        "LEFT JOIN job_listings jl ON jl.company_id = mc.id "
        "GROUP BY mc.id "
        "HAVING open_jobs > 0 "
        "ORDER BY mc.priority_score DESC LIMIT 25"
    )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_counts": {
            "sec_formd_companies": sec_count,
            "h1b_sponsors": h1b_count,
            "matched_companies": matched_count,
            "job_listings": job_count,
            "new_jobs_last_24h": new_24h,
        },
        "source_breakdown": {row["source"]: row["cnt"] for row in source_breakdown},
        "ats_discovery": {
            row["ats"]: {
                "companies_checked": row["companies"],
                "with_matching_jobs": row["with_matching_jobs"],
            }
            for row in ats_stats
        },
        "ats_breakdown": {
            row["ats_system"]: {"companies": row["companies"], "jobs": row["jobs"]}
            for row in ats_breakdown
        },
        "top_companies_with_jobs": [
            {
                "company_name": c["company_name"],
                "priority_score": c["priority_score"],
                "h1b_approvals": c["h1b_approval_count"],
                "open_jobs": c["open_jobs"],
            }
            for c in top_companies
        ],
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"  summary_report.json: {sec_count:,} SEC, {h1b_count:,} H1B, "
          f"{matched_count:,} matched, {job_count:,} jobs ({new_24h} new in 24h)")


# ─── Run All ─────────────────────────────────────────────────────────────────


def cmd_run_all(args):
    """Run all 3 phases plus export, end-to-end."""
    start = time.time()

    cmd_collect(args)
    print()
    cmd_match(args)
    print()
    cmd_scrape(args)
    print()
    cmd_export(args)

    elapsed = time.time() - start
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    print(f"\nTotal pipeline time: {minutes}m {seconds}s")


# ─── CLI ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="H1B Job Search Tool — Find H1B-sponsoring companies with open jobs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py collect                         Download and parse SEC + H1B data
  python pipeline.py match                           Match companies and compute priority scores
  python pipeline.py scrape --mode discovery -w 10   First run: discover ATS for ALL companies
  python pipeline.py scrape --mode monitor           Daily run: re-check known ATS companies
  python pipeline.py scrape --mode discovery --limit 5000 -w 10   Discovery with limit
  python pipeline.py export                          Export results to CSV/JSON
  python pipeline.py run-all --mode monitor          Run full pipeline with monitoring scrape
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Pipeline phase to run")

    # collect
    sub_collect = subparsers.add_parser("collect", help="Phase 1: Download and parse data")
    sub_collect.set_defaults(func=cmd_collect)

    # match
    sub_match = subparsers.add_parser("match", help="Phase 2: Match and score companies")
    sub_match.set_defaults(func=cmd_match)

    # scrape
    sub_scrape = subparsers.add_parser("scrape", help="Phase 3: Scrape job boards")
    sub_scrape.add_argument(
        "--mode", choices=["discovery", "monitor"], default="monitor",
        help="discovery = check ALL companies; monitor = only re-check known ATS companies (default: monitor)",
    )
    sub_scrape.add_argument(
        "--limit", type=int, default=None,
        help="Max companies to scrape (default: all in discovery, all known in monitor)",
    )
    sub_scrape.add_argument(
        "-w", "--workers", type=int, default=1,
        help="Number of concurrent workers (default: 1; recommended: 10 for discovery)",
    )
    sub_scrape.set_defaults(func=cmd_scrape)

    # export
    sub_export = subparsers.add_parser("export", help="Export results to CSV/JSON")
    sub_export.set_defaults(func=cmd_export)

    # run-all
    sub_all = subparsers.add_parser("run-all", help="Run all phases end-to-end")
    sub_all.add_argument(
        "--mode", choices=["discovery", "monitor"], default="monitor",
        help="Scraping mode (default: monitor)",
    )
    sub_all.add_argument(
        "--limit", type=int, default=None,
        help="Max companies to scrape",
    )
    sub_all.add_argument(
        "-w", "--workers", type=int, default=1,
        help="Number of concurrent workers",
    )
    sub_all.set_defaults(func=cmd_run_all)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
