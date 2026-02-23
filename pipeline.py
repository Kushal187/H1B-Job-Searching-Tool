#!/usr/bin/env python3
"""H1B Job Search Tool — Main Pipeline Orchestrator.

CLI with subcommands for each phase of the pipeline:
  collect   — Phase 1: Download and parse SEC Form D + H1B/LCA data
  match     — Phase 2: Normalize names, fuzzy-match, and score companies
  scrape    — Phase 3: Scrape Greenhouse, Lever, Ashby, and Workday for open jobs
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

    # Workday board import
    print("\n--- Workday Board Import ---")
    if os.path.exists(config.WORKDAY_URLS_CSV):
        from collectors.workday_urls import import_to_db as import_workday

        wd_count = import_workday()
    else:
        wd_count = 0
        print(f"  No Workday CSV found at {config.WORKDAY_URLS_CSV} (skipping)")

    print("\n" + "=" * 60)
    print(
        f"Collection complete: {sec_count} SEC records, {h1b_count} H1B records, "
        f"{wd_count} Workday boards"
    )
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


def _scrape_one_company(
    company: dict,
    gh_dir: str,
    lever_dir: str,
    ashby_dir: str,
    ats_filter: set[str] | None = None,
    workday_dir: str | None = None,
    workday_boards: dict | None = None,
) -> dict:
    """Scrape a single company on the requested ATS platforms. Thread-safe.

    Args:
        company: Row from matched_companies.
        gh_dir / lever_dir / ashby_dir / workday_dir: Output directories.
        ats_filter: If set, only try these ATS systems (e.g. {"ashby"}).
                    None means try all.
        workday_boards: Dict mapping company_id -> workday board config.

    Returns a result dict with keys:
      name, normalized, ats, job_count, new_job_count, status
    """
    from scrapers.base import ATSUnavailableError
    from scrapers.greenhouse import scrape_greenhouse
    from scrapers.lever import scrape_lever
    from scrapers.ashby import scrape_ashby

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

    def _fill(result, ats_name, data):
        result["ats"] = ats_name
        result["job_count"] = data.get("job_count", 0)
        result["new_job_count"] = data.get("new_job_count", 0)
        result["total_before_filter"] = data.get("total_before_filter", 0)
        result["status"] = "found" if data.get("job_count", 0) > 0 else "ats_no_match"

    # Try each ATS in turn.  ATSUnavailableError (transient failure / circuit
    # breaker) is caught per-ATS so we still try the remaining platforms.
    # The flag prevents _update_ats_status from falsely caching "not found".

    # Try Workday first if the company has a known board
    if ats_filter is None or "workday" in ats_filter:
        wb = (workday_boards or {}).get(cid)
        if wb and workday_dir:
            try:
                from scrapers.workday import scrape_workday

                wd = scrape_workday(
                    name, norm, workday_dir, company_id=cid,
                    tenant=wb["tenant"],
                    subdomain=wb["subdomain"],
                    board=wb["board"],
                )
                if wd is not None:
                    _fill(result, "workday", wd)
                    return result
            except ATSUnavailableError:
                result["had_transient_errors"] = True

    # Try Greenhouse
    if ats_filter is None or "greenhouse" in ats_filter:
        try:
            gh = scrape_greenhouse(name, norm, gh_dir, company_id=cid)
            if gh is not None:
                _fill(result, "greenhouse", gh)
                return result
        except ATSUnavailableError:
            result["had_transient_errors"] = True

    # Try Lever
    if ats_filter is None or "lever" in ats_filter:
        try:
            lv = scrape_lever(name, norm, lever_dir, company_id=cid)
            if lv is not None:
                _fill(result, "lever", lv)
                return result
        except ATSUnavailableError:
            result["had_transient_errors"] = True

    # Try Ashby
    if ats_filter is None or "ashby" in ats_filter:
        try:
            ash = scrape_ashby(name, norm, ashby_dir, company_id=cid)
            if ash is not None:
                _fill(result, "ashby", ash)
                return result
        except ATSUnavailableError:
            result["had_transient_errors"] = True

    return result


def _update_ats_status(result: dict, ats_filter: set[str] | None = None):
    """Cache the ATS status for a company in company_ats_status table.

    When *ats_filter* is set and the result is "not_found", we only update
    the status if the company does NOT already have a known ATS that wasn't
    in our filter.  This avoids overwriting ``has_jobs`` for Greenhouse/Lever
    companies when we only checked Ashby.
    """
    now = datetime.now(timezone.utc).isoformat()
    has_jobs = 1 if result["job_count"] > 0 else 0

    if result["ats"]:
        # Found on an ATS — always update (new discovery or refresh)
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
        # Not found on the checked ATS systems.
        # If any ATS had a transient error (timeout, 5xx, circuit breaker),
        # don't cache "not found" — we can't be sure the board doesn't exist.
        if result.get("had_transient_errors"):
            return

        # If we only checked a subset (e.g. --ats ashby) and the company is
        # already known on a different ATS, skip the update to avoid
        # overwriting has_jobs / ats_system with NULL / 0.
        if ats_filter:
            existing = database.query(
                "SELECT ats_system FROM company_ats_status WHERE normalized_name = ?",
                (result["normalized"],),
            )
            if (
                existing
                and existing[0]["ats_system"]
                and existing[0]["ats_system"] not in ats_filter
            ):
                # Company already has a known ATS outside our filter — don't touch it
                return

        database.execute(
            """INSERT INTO company_ats_status (company_id, normalized_name, ats_system, last_checked, has_jobs)
               VALUES (?, ?, NULL, ?, 0)
               ON CONFLICT(normalized_name) DO UPDATE SET
                   last_checked = excluded.last_checked,
                   has_jobs = 0""",
            (result["company_id"], result["normalized"], now),
        )


def parse_ats_filter(ats_raw: str | None) -> set[str] | None:
    """Parse and validate a comma-separated ATS filter string.

    Returns:
        A set of ATS names, or ``None`` if no filter was provided.

    Raises:
        ValueError: If *ats_raw* contains unrecognised ATS names.
    """
    if not ats_raw:
        return None
    ats_filter = {a.strip().lower() for a in ats_raw.split(",")}
    valid = {"greenhouse", "lever", "ashby", "workday"}
    unknown = ats_filter - valid
    if unknown:
        raise ValueError(f"Unknown ATS systems: {unknown}. Valid: {valid}")
    return ats_filter


def get_companies_to_scrape(
    mode: str,
    ats_filter: set[str] | None = None,
    limit: int | None = None,
) -> list[dict]:
    """Query the database for companies to scrape.

    Args:
        mode: ``"discovery"`` (all matched companies) or ``"monitor"``
              (only companies previously found on an ATS).
        ats_filter: Optional set of ATS names to restrict the monitor query.
        limit: Maximum number of companies (discovery mode only).

    Returns:
        List of company row dicts with keys
        ``id``, ``company_name``, ``normalized_name``, ``priority_score``.
    """
    if mode == "discovery":
        sql = (
            "SELECT id, company_name, normalized_name, priority_score "
            "FROM matched_companies ORDER BY priority_score DESC"
        )
        params: tuple = ()
        if limit:
            sql += " LIMIT ?"
            params = (limit,)
        return database.query(sql, params)

    if mode == "monitor":
        ats_where = ""
        if ats_filter:
            placeholders = ",".join(["?"] * len(ats_filter))
            ats_where = f"AND cas.ats_system IN ({placeholders})"
        return database.query(
            f"""SELECT mc.id, mc.company_name, mc.normalized_name, mc.priority_score
               FROM matched_companies mc
               INNER JOIN company_ats_status cas ON cas.normalized_name = mc.normalized_name
               WHERE cas.ats_system IS NOT NULL {ats_where}
               ORDER BY mc.priority_score DESC""",
            tuple(ats_filter) if ats_filter else (),
        )

    return []


def run_scrape(
    mode: str = "monitor",
    workers: int = 1,
    ats_filter: set[str] | None = None,
    limit: int | None = None,
    on_start=None,
    on_result=None,
    on_error=None,
) -> dict:
    """Core scrape orchestration used by both the CLI and the web admin UI.

    Queries companies, fans out scraping across workers, calls
    ``_scrape_one_company`` / ``_update_ats_status`` for each, and tallies
    aggregate statistics.

    Args:
        mode: ``"discovery"`` or ``"monitor"``.
        workers: Number of concurrent threads (1 = sequential).
        ats_filter: Only try these ATS systems (``None`` = all).
        limit: Cap the number of companies (discovery only).
        on_start: ``(total: int) -> None`` — called once with the company
                  count before scraping begins.
        on_result: ``(result: dict, completed: int, total: int) -> None`` —
                   called after each company finishes successfully.
        on_error: ``(company: dict, error: Exception, completed: int,
                  total: int) -> None`` — called when a company scrape raises.

    Returns:
        Stats dict with keys: ``greenhouse``, ``lever``, ``ashby``,
        ``ats_no_match``, ``not_found``, ``total_jobs``, ``new_jobs``,
        ``total``, ``elapsed``.
    """
    # SQLite needs local schema init; Postgres schema is managed separately.
    if not database.using_postgres():
        database.init_db()

    companies = get_companies_to_scrape(mode, ats_filter, limit)

    # If Workday is in scope, also include companies from workday_boards
    # that might not be in matched_companies yet (or not in the initial query).
    workday_boards = _load_workday_boards(ats_filter)
    if workday_boards and mode == "monitor":
        existing_ids = {c["id"] for c in companies}
        # Add Workday companies that aren't already in the list
        wb_company_ids = [
            cid for cid in workday_boards if cid and cid not in existing_ids
        ]
        if wb_company_ids:
            placeholders = ",".join(["?"] * len(wb_company_ids))
            extra = database.query(
                f"SELECT id, company_name, normalized_name, priority_score "
                f"FROM matched_companies WHERE id IN ({placeholders}) "
                f"ORDER BY priority_score DESC",
                tuple(wb_company_ids),
            )
            companies.extend(extra)

    total = len(companies)

    stats = {
        "greenhouse": 0,
        "lever": 0,
        "ashby": 0,
        "workday": 0,
        "ats_no_match": 0,
        "not_found": 0,
        "total_jobs": 0,
        "new_jobs": 0,
    }

    if on_start:
        on_start(total)

    if not companies:
        stats["total"] = 0
        stats["elapsed"] = 0.0
        return stats

    os.makedirs(config.GREENHOUSE_DIR, exist_ok=True)
    os.makedirs(config.LEVER_DIR, exist_ok=True)
    os.makedirs(config.ASHBY_DIR, exist_ok=True)
    os.makedirs(config.WORKDAY_DIR, exist_ok=True)

    start_time = time.time()
    completed = 0

    def _process(company):
        result = _scrape_one_company(
            company,
            config.GREENHOUSE_DIR,
            config.LEVER_DIR,
            config.ASHBY_DIR,
            ats_filter=ats_filter,
            workday_dir=config.WORKDAY_DIR,
            workday_boards=workday_boards,
        )
        _update_ats_status(result, ats_filter=ats_filter)
        return result

    if workers <= 1:
        for company in companies:
            completed += 1
            try:
                result = _process(company)
                _tally_stats(stats, result)
                if on_result:
                    on_result(result, completed, total)
            except Exception as e:
                stats["not_found"] += 1
                if on_error:
                    on_error(company, e, completed, total)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_process, c): c for c in companies
            }
            for future in as_completed(futures):
                completed += 1
                try:
                    result = future.result()
                    _tally_stats(stats, result)
                    if on_result:
                        on_result(result, completed, total)
                except Exception as e:
                    company = futures[future]
                    stats["not_found"] += 1
                    if on_error:
                        on_error(company, e, completed, total)

    stats["total"] = total
    stats["elapsed"] = time.time() - start_time
    return stats


def cmd_scrape(args):
    """Scrape Greenhouse, Lever, Ashby, and Workday for open jobs at matched companies."""
    mode = getattr(args, "mode", "monitor")
    workers = getattr(args, "workers", 1)
    limit = getattr(args, "limit", None)
    ats_raw = getattr(args, "ats", None)

    try:
        ats_filter = parse_ats_filter(ats_raw)
    except ValueError as e:
        print(e)
        return

    ats_label = ",".join(sorted(ats_filter)) if ats_filter else "all"
    print("=" * 60)
    print(f"PHASE 3: Job Scraping (mode={mode}, ats={ats_label}, workers={workers})")
    print("=" * 60)

    start_time = time.time()

    def on_start(total):
        if total == 0:
            if mode == "monitor":
                print("\nNo companies with known ATS found.")
                print(
                    "Run with --mode discovery first to identify which "
                    "companies use an ATS."
                )
            else:
                print("No matched companies found. Run 'match' first.")
        else:
            print(f"\nScraping {total:,} companies with {workers} worker(s)...\n")

    def on_result(result, completed, total):
        _print_progress(completed, total, result, start_time)

    def on_error(company, error, completed, total):
        print(
            f"  [{completed}/{total}] {company['company_name']} -> ERROR: {error}"
        )

    stats = run_scrape(
        mode, workers, ats_filter, limit, on_start, on_result, on_error,
    )

    if stats["total"] > 0:
        _print_summary(stats, stats["total"], stats["elapsed"])


def _print_progress(i: int, total: int, result: dict, start_time: float):
    """Print a single-line progress update."""
    elapsed = time.time() - start_time
    rate = i / elapsed if elapsed > 0 else 0
    eta_seconds = (total - i) / rate if rate > 0 else 0
    eta_min = int(eta_seconds // 60)
    eta_sec = int(eta_seconds % 60)

    name = result["name"][:40]
    if result["status"] == "found":
        new_tag = (
            f" ({result['new_job_count']} new)" if result.get("new_job_count") else ""
        )
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


def _load_workday_boards(ats_filter: set[str] | None = None) -> dict:
    """Load Workday board configs from the database.

    Returns a dict mapping company_id -> {tenant, subdomain, board}.
    Returns empty dict if Workday is excluded by the ATS filter.
    """
    if ats_filter and "workday" not in ats_filter:
        return {}

    rows = database.query(
        "SELECT company_id, tenant, subdomain, board FROM workday_boards "
        "WHERE company_id IS NOT NULL"
    )
    return {
        r["company_id"]: {
            "tenant": r["tenant"],
            "subdomain": r["subdomain"],
            "board": r["board"],
        }
        for r in rows
    }


def _tally_stats(stats: dict, result: dict):
    """Accumulate scraping statistics."""
    if result["status"] == "found":
        ats = result["ats"]
        if ats in stats:
            stats[ats] += 1
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

    found = (
        stats["greenhouse"] + stats["lever"] + stats["ashby"] + stats["workday"]
    )
    print(f"\n{'=' * 60}")
    print(f"Scraping complete in {minutes}m {seconds}s")
    print(f"{'=' * 60}")
    print(f"  Companies scraped:   {total:,}")
    print(f"  Greenhouse matches:  {stats['greenhouse']:,}")
    print(f"  Lever matches:       {stats['lever']:,}")
    print(f"  Ashby matches:       {stats['ashby']:,}")
    print(f"  Workday matches:     {stats['workday']:,}")
    print(f"  ATS found, 0 match:  {stats['ats_no_match']:,}")
    print(f"  Not on any ATS:      {stats['not_found']:,}")
    print(
        f"  Match rate:          {found / total * 100:.1f}%" if total > 0 else "  N/A"
    )
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
    rows = database.query(
        """
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
    """
    )

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
    rows = database.query(
        """
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
    """
    )

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
    sec_count = database.query("SELECT COUNT(*) as cnt FROM sec_formd_companies")[0][
        "cnt"
    ]
    h1b_count = database.query("SELECT COUNT(*) as cnt FROM h1b_sponsors")[0]["cnt"]
    matched_count = database.query("SELECT COUNT(*) as cnt FROM matched_companies")[0][
        "cnt"
    ]
    job_count = database.query("SELECT COUNT(*) as cnt FROM job_listings")[0]["cnt"]

    # ATS discovery stats
    ats_stats = database.query(
        """
        SELECT
            COALESCE(ats_system, 'not_found') as ats,
            COUNT(*) as companies,
            SUM(has_jobs) as with_matching_jobs
        FROM company_ats_status
        GROUP BY ats_system
    """
    )

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

    print(
        f"  summary_report.json: {sec_count:,} SEC, {h1b_count:,} H1B, "
        f"{matched_count:,} matched, {job_count:,} jobs ({new_24h} new in 24h)"
    )


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
  python pipeline.py scrape --mode discovery --ats ashby -w 15   Discover Ashby boards only
  python pipeline.py scrape --mode monitor --ats greenhouse,lever   Monitor Greenhouse + Lever only
  python pipeline.py scrape --mode monitor --ats workday -w 5       Scrape Workday boards only
  python pipeline.py scrape --mode discovery --limit 5000 -w 10   Discovery with limit
  python pipeline.py export                          Export results to CSV/JSON
  python pipeline.py run-all --mode monitor          Run full pipeline with monitoring scrape
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Pipeline phase to run")

    # collect
    sub_collect = subparsers.add_parser(
        "collect", help="Phase 1: Download and parse data"
    )
    sub_collect.set_defaults(func=cmd_collect)

    # match
    sub_match = subparsers.add_parser(
        "match", help="Phase 2: Match and score companies"
    )
    sub_match.set_defaults(func=cmd_match)

    # scrape
    sub_scrape = subparsers.add_parser("scrape", help="Phase 3: Scrape job boards")
    sub_scrape.add_argument(
        "--mode",
        choices=["discovery", "monitor"],
        default="monitor",
        help="discovery = check ALL companies; monitor = only re-check known ATS companies (default: monitor)",
    )
    sub_scrape.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max companies to scrape (default: all in discovery, all known in monitor)",
    )
    sub_scrape.add_argument(
        "-w",
        "--workers",
        type=int,
        default=1,
        help="Number of concurrent workers (default: 1; recommended: 10 for discovery)",
    )
    sub_scrape.add_argument(
        "--ats",
        type=str,
        default=None,
        help="Comma-separated ATS systems to check (greenhouse,lever,ashby,workday). Default: all",
    )
    sub_scrape.set_defaults(func=cmd_scrape)

    # export
    sub_export = subparsers.add_parser("export", help="Export results to CSV/JSON")
    sub_export.set_defaults(func=cmd_export)

    # run-all
    sub_all = subparsers.add_parser("run-all", help="Run all phases end-to-end")
    sub_all.add_argument(
        "--mode",
        choices=["discovery", "monitor"],
        default="monitor",
        help="Scraping mode (default: monitor)",
    )
    sub_all.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max companies to scrape",
    )
    sub_all.add_argument(
        "-w",
        "--workers",
        type=int,
        default=1,
        help="Number of concurrent workers",
    )
    sub_all.add_argument(
        "--ats",
        type=str,
        default=None,
        help="Comma-separated ATS systems to check (greenhouse,lever,ashby,workday). Default: all",
    )
    sub_all.set_defaults(func=cmd_run_all)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
