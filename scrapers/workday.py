"""Workday CXS API scraper.

Scrapes job listings from Workday career pages using the JSON-based CXS API.
Unlike Greenhouse/Lever/Ashby, Workday uses POST requests with offset-based
pagination and requires tenant/subdomain/board config per company.

API endpoint:
    POST https://{tenant}.{subdomain}.myworkdayjobs.com/wday/cxs/{tenant}/{board}/jobs
    Body: {"appliedFacets": {}, "limit": 20, "offset": N, "searchText": ""}
"""

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests

import config
from scrapers.base import ATSUnavailableError, BaseScraper
from scrapers.location_filter import is_usa_location
from scrapers.title_filter import is_target_role

# Regex to extract country from externalPath like /job/US-CA-Santa-Clara/...
_PATH_COUNTRY_RE = re.compile(r"^/job/([A-Z]{2,3})-")

# Regex to extract multi-location count like "2 Locations"
_MULTI_LOC_RE = re.compile(r"^\d+\s+Locations?$", re.IGNORECASE)


def _parse_posted_on(posted_on: str) -> str:
    """Convert Workday relative date string to ISO date.

    Examples:
        "Posted Today"       -> today's date
        "Posted Yesterday"   -> yesterday
        "Posted 2 Days Ago"  -> 2 days ago
        "Posted 30+ Days Ago" -> 30 days ago
    """
    now = datetime.now(timezone.utc)

    if not posted_on:
        return now.strftime("%Y-%m-%d")

    lower = posted_on.lower().strip()

    if "today" in lower:
        return now.strftime("%Y-%m-%d")
    if "yesterday" in lower:
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")

    # "Posted 2 Days Ago" / "Posted 30+ Days Ago"
    m = re.search(r"(\d+)\+?\s*days?\s*ago", lower)
    if m:
        days = int(m.group(1))
        return (now - timedelta(days=days)).strftime("%Y-%m-%d")

    return now.strftime("%Y-%m-%d")


def _extract_location(posting: dict) -> str:
    """Extract a usable location string from a Workday job posting.

    Uses ``locationsText`` if it's a concrete location.  When it's a
    generic "N Locations" string, falls back to parsing the primary
    location from ``externalPath``.
    """
    loc_text = posting.get("locationsText", "")

    # If it's a specific location (not "N Locations"), use it directly
    if loc_text and not _MULTI_LOC_RE.match(loc_text):
        return loc_text

    # Fall back: parse from externalPath like /job/US-CA-Santa-Clara/...
    path = posting.get("externalPath", "")
    parts = path.split("/")
    if len(parts) >= 3 and parts[1] == "job":
        # parts[2] is e.g. "US-CA-Santa-Clara" or "Israel-Yokneam"
        location_slug = parts[2]
        # Convert hyphens to ", " for readability
        tokens = location_slug.split("-")
        return ", ".join(tokens)

    return loc_text or "Unknown"


class WorkdayScraper(BaseScraper):
    """Workday CXS API scraper with POST-based pagination."""

    ats_name = "workday"
    board_url_template = "https://{tenant}.{subdomain}.myworkdayjobs.com/{board}"

    # These satisfy the ABC contract but aren't used in the overridden scrape()
    def get_api_url(self, normalized: str) -> str:
        # Actual URL construction happens in scrape() using workday_boards lookup
        return ""

    def extract_all_jobs(self, response_json) -> list[dict]:
        return response_json.get("jobPostings", [])

    def is_job_relevant(self, job: dict) -> bool:
        location = _extract_location(job)
        title = job.get("title", "")
        return is_usa_location(location) and is_target_role(title)

    def wrap_for_save(self, jobs: list[dict]) -> dict:
        return {"jobs": jobs}

    def extract_job_fields(self, job: dict, tenant: str = "", subdomain: str = "",
                           board: str = "") -> dict:
        """Extract normalised fields from a Workday job posting.

        Args:
            job: Raw job dict from jobPostings array.
            tenant/subdomain/board: Needed to construct the full job URL.
        """
        location = _extract_location(job)
        external_path = job.get("externalPath", "")

        # Construct full job URL
        if tenant and subdomain and board and external_path:
            job_url = (
                f"https://{tenant}.{subdomain}.myworkdayjobs.com"
                f"/{board}{external_path}"
            )
        else:
            job_url = external_path

        posted_at = _parse_posted_on(job.get("postedOn", ""))

        # bulletFields[0] is typically the job requisition ID
        bullet = job.get("bulletFields", [])
        department = bullet[0] if bullet else ""

        return {
            "title": job.get("title", ""),
            "location": location,
            "department": department,
            "job_url": job_url,
            "posted_at": posted_at,
        }

    # ── Workday-specific pagination fetch ─────────────────────────────────

    def _fetch_all_postings(
        self, tenant: str, subdomain: str, board: str
    ) -> list[dict]:
        """Fetch all job postings from a Workday board using paginated POST.

        Returns the full list of raw job posting dicts.

        Raises:
            ATSUnavailableError: On connection/timeout or repeated HTTP errors.
        """
        base_url = (
            f"https://{tenant}.{subdomain}.myworkdayjobs.com"
            f"/wday/cxs/{tenant}/{board}/jobs"
        )
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        page_size = config.WORKDAY_PAGE_SIZE
        max_pages = config.WORKDAY_MAX_PAGES
        all_postings: list[dict] = []
        offset = 0
        known_total: int | None = None  # captured from first page

        for page in range(max_pages):
            body = {
                "appliedFacets": {},
                "limit": page_size,
                "offset": offset,
                "searchText": "",
            }

            # Use retry logic for resilience
            resp = None
            last_error = ""
            for attempt in range(config.SCRAPE_MAX_RETRIES + 1):
                try:
                    resp = requests.post(
                        base_url,
                        json=body,
                        headers=headers,
                        timeout=config.SCRAPE_TIMEOUT,
                    )

                    if resp.status_code == 200:
                        self._record_success()
                        break

                    if resp.status_code in (404, 410):
                        # Board doesn't exist
                        return []

                    if resp.status_code in (429, 500, 502, 503, 504):
                        self._record_failure()
                        last_error = f"HTTP {resp.status_code}"
                        if attempt < config.SCRAPE_MAX_RETRIES:
                            wait = self._backoff(resp, attempt)
                            time.sleep(wait)
                            continue
                        raise ATSUnavailableError(
                            self.ats_name,
                            f"HTTP {resp.status_code} after "
                            f"{config.SCRAPE_MAX_RETRIES} retries",
                        )

                    # Other non-retryable error (e.g. 403, 422)
                    self._record_success()  # API is alive, just rejected
                    return []

                except ATSUnavailableError:
                    raise
                except (requests.ConnectionError, requests.Timeout) as exc:
                    self._record_failure()
                    last_error = f"{type(exc).__name__}: {exc}"
                    if attempt < config.SCRAPE_MAX_RETRIES:
                        wait = config.SCRAPE_RETRY_BACKOFF * (2 ** attempt)
                        time.sleep(wait)
                        continue
                    raise ATSUnavailableError(
                        self.ats_name,
                        f"Failed after {config.SCRAPE_MAX_RETRIES} retries: "
                        f"{last_error}",
                    ) from exc
            else:
                # All retries exhausted without break
                raise ATSUnavailableError(self.ats_name, last_error)

            data = resp.json()
            postings = data.get("jobPostings", [])

            # Workday only returns total on the first page (offset=0).
            # Capture it once and use for all subsequent pagination checks.
            page_total = data.get("total", 0)
            if page_total > 0 and known_total is None:
                known_total = page_total

            all_postings.extend(postings)
            offset += len(postings)

            # Stop if we've fetched all jobs or got an empty page
            if not postings:
                break
            if known_total is not None and offset >= known_total:
                break

            # Rate-limit between pages
            time.sleep(config.SCRAPE_DELAY)

        return all_postings

    # ── Override scrape() for Workday-specific flow ───────────────────────

    def scrape(
        self,
        company_name: str,
        normalized: str,
        output_dir: str,
        delay: float = config.SCRAPE_DELAY,
        save_to_db: bool = True,
        company_id: int | None = None,
        db_conn=None,
        *,
        tenant: str = "",
        subdomain: str = "",
        board: str = "",
    ) -> dict | None:
        """Scrape a Workday board for job listings.

        Workday requires ``tenant``, ``subdomain``, and ``board`` params
        (looked up from the ``workday_boards`` table by the caller).
        If not provided, attempts a DB lookup using *normalized*.
        """
        # Look up Workday config if not provided
        if not (tenant and subdomain and board):
            wb = self._lookup_workday_board(normalized, company_id)
            if wb is None:
                return None
            tenant = wb["tenant"]
            subdomain = wb["subdomain"]
            board = wb["board"]

        board_url = (
            f"https://{tenant}.{subdomain}.myworkdayjobs.com/{board}"
        )
        scraped_at = datetime.now(timezone.utc).isoformat()

        try:
            # Circuit breaker gate
            if not self._circuit_allows_request():
                raise ATSUnavailableError(
                    self.ats_name,
                    f"circuit breaker open "
                    f"(>{config.SCRAPE_CIRCUIT_BREAKER_THRESHOLD} "
                    f"consecutive failures)",
                )

            # Fetch all pages
            all_postings = self._fetch_all_postings(tenant, subdomain, board)

            if not all_postings:
                if save_to_db and company_id:
                    self._deactivate_stale_jobs(company_id, scraped_at, db_conn)
                return {
                    "original_name": company_name,
                    "normalized_name": normalized,
                    "job_count": 0,
                    "total_before_filter": 0,
                    "ats": self.ats_name,
                    "url": board_url,
                    "scraped_at": scraped_at,
                }

            # Filter to relevant US entry-level jobs
            jobs = [j for j in all_postings if self.is_job_relevant(j)]

            if jobs:
                # Save to filesystem
                company_dir = os.path.join(output_dir, normalized)
                os.makedirs(company_dir, exist_ok=True)

                with open(os.path.join(company_dir, "jobs.json"), "w") as f:
                    json.dump(self.wrap_for_save(jobs), f, indent=2)

                metadata = {
                    "original_name": company_name,
                    "normalized_name": normalized,
                    "job_count": len(jobs),
                    "total_before_filter": len(all_postings),
                    "ats": self.ats_name,
                    "url": board_url,
                    "scraped_at": scraped_at,
                }
                with open(os.path.join(company_dir, "metadata.json"), "w") as f:
                    json.dump(metadata, f, indent=2)

                if save_to_db:
                    new_count = self._upsert_workday_jobs(
                        company_name, jobs, scraped_at,
                        company_id, db_conn,
                        tenant, subdomain, board,
                    )
                    metadata["new_job_count"] = new_count
                    self._deactivate_stale_jobs(company_id, scraped_at, db_conn)

                # Update last_scraped in workday_boards
                self._update_last_scraped(tenant, board, scraped_at, len(jobs))

                return metadata

            # ATS found but no matching jobs after filtering
            if save_to_db and company_id:
                self._deactivate_stale_jobs(company_id, scraped_at, db_conn)

            self._update_last_scraped(tenant, board, scraped_at, 0)

            return {
                "original_name": company_name,
                "normalized_name": normalized,
                "job_count": 0,
                "total_before_filter": len(all_postings),
                "ats": self.ats_name,
                "url": board_url,
                "scraped_at": scraped_at,
            }

        except ATSUnavailableError:
            raise

        except Exception as e:
            print(f"  Error scraping workday for {normalized}: {e}")
            return None

        finally:
            time.sleep(delay)

    # ── DB helpers ────────────────────────────────────────────────────────

    def _upsert_workday_jobs(
        self,
        company_name: str,
        jobs: list[dict],
        scraped_at: str,
        company_id: int | None,
        db_conn,
        tenant: str,
        subdomain: str,
        board: str,
    ) -> int:
        """Insert/update Workday jobs, passing tenant info to extract_job_fields."""
        from db import database

        # Reuse one connection for the whole company/ATS upsert to avoid
        # opening a new DB connection per job row.
        if db_conn is None:
            with database.get_db() as conn:
                return self._upsert_workday_jobs(
                    company_name=company_name,
                    jobs=jobs,
                    scraped_at=scraped_at,
                    company_id=company_id,
                    db_conn=conn,
                    tenant=tenant,
                    subdomain=subdomain,
                    board=board,
                )

        upsert_sql = database.adapt_sql(
            """
            INSERT INTO job_listings
                (company_id, company_name, ats_system, job_title, job_location,
                 job_url, department, scraped_at, first_seen_at, last_seen_at,
                 posted_at, is_active, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(job_url) DO UPDATE SET
                scraped_at = excluded.scraped_at,
                last_seen_at = excluded.last_seen_at,
                is_active = 1,
                raw_json = excluded.raw_json
        """
        )
        check_sql = database.adapt_sql(
            "SELECT 1 FROM job_listings WHERE job_url = ? LIMIT 1"
        )

        new_count = 0
        for job in jobs:
            fields = self.extract_job_fields(
                job, tenant=tenant, subdomain=subdomain, board=board,
            )
            job_url = fields["job_url"]

            params = (
                company_id,
                company_name,
                self.ats_name,
                fields["title"],
                fields["location"],
                job_url,
                fields["department"],
                scraped_at,
                scraped_at,  # first_seen_at
                scraped_at,  # last_seen_at
                fields["posted_at"],
                json.dumps(job),
            )

            try:
                exists = db_conn.execute(check_sql, (job_url,)).fetchone()
                db_conn.execute(upsert_sql, params)
                if not exists:
                    new_count += 1
            except Exception as e:
                print(f"    DB error inserting workday job {job_url}: {e}")

        return new_count

    @staticmethod
    def _lookup_workday_board(
        normalized: str, company_id: int | None = None
    ) -> dict | None:
        """Look up tenant/subdomain/board from the workday_boards table."""
        from db import database

        # Try by company_id first (most precise)
        if company_id:
            rows = database.query(
                "SELECT tenant, subdomain, board FROM workday_boards "
                "WHERE company_id = ? LIMIT 1",
                (company_id,),
            )
            if rows:
                return rows[0]

        # Fallback: by normalized_name
        rows = database.query(
            "SELECT tenant, subdomain, board FROM workday_boards "
            "WHERE normalized_name = ? LIMIT 1",
            (normalized,),
        )
        return rows[0] if rows else None

    @staticmethod
    def _update_last_scraped(
        tenant: str, board: str, scraped_at: str, job_count: int
    ):
        """Update the last_scraped timestamp and job_count in workday_boards."""
        from db import database

        database.execute(
            "UPDATE workday_boards SET last_scraped = ?, job_count = ? "
            "WHERE tenant = ? AND board = ?",
            (scraped_at, job_count, tenant, board),
        )


# ── Module-level convenience function (matches pattern of other scrapers) ──

_scraper = WorkdayScraper()


def scrape_workday(
    company_name: str,
    normalized: str,
    output_dir: str,
    delay: float = config.SCRAPE_DELAY,
    save_to_db: bool = True,
    company_id: int | None = None,
    db_conn=None,
    *,
    tenant: str = "",
    subdomain: str = "",
    board: str = "",
) -> dict | None:
    """Query Workday CXS API for a company's open jobs."""
    return _scraper.scrape(
        company_name,
        normalized,
        output_dir,
        delay=delay,
        save_to_db=save_to_db,
        company_id=company_id,
        db_conn=db_conn,
        tenant=tenant,
        subdomain=subdomain,
        board=board,
    )
