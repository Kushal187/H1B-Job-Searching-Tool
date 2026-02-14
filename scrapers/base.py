"""Base ATS scraper with shared logic for all job-board APIs.

Subclasses need only define the ATS-specific field mappings, URL templates,
and filtering rules.  All database I/O, filesystem persistence, rate-limiting,
retry logic, circuit breaking, and error handling live here in one place.
"""

import json
import os
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import requests

import config

# ── HTTP status-code categories ──────────────────────────────────────────────

# Board genuinely does not exist — no retry, count as "not found".
_NOT_FOUND_CODES = frozenset({404, 410})

# Transient server / rate-limit errors — worth retrying.
_RETRYABLE_CODES = frozenset({429, 500, 502, 503, 504})


# ── Custom exception ────────────────────────────────────────────────────────


class ATSUnavailableError(Exception):
    """Raised when an ATS API is unreachable after retries or circuit-broken.

    Callers should catch this to distinguish *"company not on this ATS"*
    (``scrape()`` returns ``None``) from *"we couldn't reach the ATS at all"*.
    """

    def __init__(self, ats_name: str, reason: str):
        self.ats_name = ats_name
        self.reason = reason
        super().__init__(f"{ats_name} unavailable: {reason}")


# ── Base class ──────────────────────────────────────────────────────────────


class BaseScraper(ABC):
    """Abstract base for ATS job-board scrapers.

    Subclasses must set class attributes and implement the abstract methods
    that capture the small differences between Greenhouse, Lever, Ashby, etc.
    """

    # ── Subclasses MUST set these ────────────────────────────────────────
    ats_name: str  # e.g. "greenhouse"
    board_url_template: str  # e.g. "https://boards.greenhouse.io/{slug}"

    # ── Optional overrides ───────────────────────────────────────────────
    skip_if_board_empty: bool = False  # True for Ashby (empty board → None)

    def __init__(self):
        # Circuit-breaker state (thread-safe via lock)
        self._cb_lock = threading.Lock()
        self._consecutive_failures: int = 0
        self._circuit_open_until: float = 0.0  # epoch timestamp

    # ── Abstract hooks (the *only* things that differ per ATS) ───────────

    @abstractmethod
    def get_api_url(self, normalized: str) -> str:
        """Return the full API endpoint URL for *normalized* company slug."""

    @abstractmethod
    def extract_all_jobs(self, response_json) -> list[dict]:
        """Pull the raw job list out of the parsed API response."""

    @abstractmethod
    def is_job_relevant(self, job: dict) -> bool:
        """Return True if *job* passes location + title filters."""

    @abstractmethod
    def wrap_for_save(self, jobs: list[dict]) -> dict | list:
        """Return the structure to dump as ``jobs.json``."""

    @abstractmethod
    def extract_job_fields(self, job: dict) -> dict:
        """Extract normalised fields from a single job dict.

        Must return a dict with keys:
            title, location, department, job_url, posted_at
        """

    # ── Circuit breaker ──────────────────────────────────────────────────

    def _circuit_allows_request(self) -> bool:
        """Return ``True`` if the circuit is closed (or half-open after cooldown)."""
        with self._cb_lock:
            if self._consecutive_failures < config.SCRAPE_CIRCUIT_BREAKER_THRESHOLD:
                return True  # circuit closed
            # Circuit is tripped — check if cooldown has elapsed
            if time.time() >= self._circuit_open_until:
                return True  # half-open: allow one probe request
            return False  # still open — fail fast

    def _record_success(self):
        """Reset the failure counter (circuit → closed)."""
        with self._cb_lock:
            self._consecutive_failures = 0

    def _record_failure(self):
        """Increment the failure counter; trip the circuit if threshold reached."""
        with self._cb_lock:
            self._consecutive_failures += 1
            if self._consecutive_failures >= config.SCRAPE_CIRCUIT_BREAKER_THRESHOLD:
                self._circuit_open_until = (
                    time.time() + config.SCRAPE_CIRCUIT_BREAKER_COOLDOWN
                )
                print(
                    f"  ⚠ Circuit breaker OPEN for {self.ats_name}: "
                    f"{self._consecutive_failures} consecutive failures — "
                    f"pausing for {config.SCRAPE_CIRCUIT_BREAKER_COOLDOWN}s"
                )

    # ── HTTP request with retry ──────────────────────────────────────────

    def _request_with_retry(self, url: str) -> requests.Response:
        """GET *url* with automatic retry + exponential back-off.

        * 200 / 404 / 410 / other non-retryable codes → return ``Response``.
        * 429 / 5xx / timeout / connection error → retry up to
          ``config.SCRAPE_MAX_RETRIES`` times, then raise
          :class:`ATSUnavailableError`.
        """
        max_retries = config.SCRAPE_MAX_RETRIES
        last_error: str = ""

        for attempt in range(max_retries + 1):
            try:
                resp = requests.get(url, timeout=config.SCRAPE_TIMEOUT)

                if resp.status_code in _RETRYABLE_CODES:
                    self._record_failure()
                    last_error = f"HTTP {resp.status_code}"

                    if attempt < max_retries:
                        wait = self._backoff(resp, attempt)
                        print(
                            f"    {self.ats_name}: HTTP {resp.status_code} — "
                            f"retry {attempt + 1}/{max_retries} in {wait:.1f}s"
                        )
                        time.sleep(wait)
                        continue

                    # All retries exhausted on a retryable status
                    raise ATSUnavailableError(
                        self.ats_name,
                        f"HTTP {resp.status_code} after {max_retries} retries",
                    )

                # Non-retryable response (200, 404, 403, etc.) — API is alive.
                self._record_success()
                return resp

            except ATSUnavailableError:
                raise  # don't swallow our own exception

            except (requests.ConnectionError, requests.Timeout) as exc:
                self._record_failure()
                last_error = f"{type(exc).__name__}: {exc}"

                if attempt < max_retries:
                    wait = config.SCRAPE_RETRY_BACKOFF * (2**attempt)
                    print(
                        f"    {self.ats_name}: {type(exc).__name__} — "
                        f"retry {attempt + 1}/{max_retries} in {wait:.1f}s"
                    )
                    time.sleep(wait)
                    continue

                raise ATSUnavailableError(
                    self.ats_name,
                    f"Failed after {max_retries} retries: {last_error}",
                ) from exc

        # Should be unreachable, but be safe
        raise ATSUnavailableError(self.ats_name, last_error)  # pragma: no cover

    @staticmethod
    def _backoff(resp: requests.Response, attempt: int) -> float:
        """Compute back-off seconds, respecting ``Retry-After`` on 429."""
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    return min(float(retry_after), 30.0)
                except (ValueError, TypeError):
                    pass
        return config.SCRAPE_RETRY_BACKOFF * (2**attempt)

    # ── Shared scraping flow ─────────────────────────────────────────────

    def scrape(
        self,
        company_name: str,
        normalized: str,
        output_dir: str,
        delay: float = config.SCRAPE_DELAY,
        save_to_db: bool = True,
        company_id: int | None = None,
        db_conn=None,
    ) -> dict | None:
        """Query the ATS API and persist matching jobs.

        Args:
            company_name: Original company name for display.
            normalized: Normalized slug used as board name.
            output_dir: Base directory for saving JSON output.
            delay: Seconds to wait after the request (rate limiting).
            save_to_db: Whether to insert job records into the database.
            company_id: matched_companies.id for DB foreign key.
            db_conn: Optional database connection (for thread-safe writes).

        Returns:
            Metadata dict when the company is found on this ATS,
            ``None`` when the board does not exist (404).

        Raises:
            ATSUnavailableError: When the ATS API is unreachable after
                retries, or the circuit breaker is open.
        """
        url = self.get_api_url(normalized)
        board_url = self.board_url_template.format(slug=normalized)
        made_request = False

        try:
            # ── Circuit breaker gate ─────────────────────────────────────
            if not self._circuit_allows_request():
                raise ATSUnavailableError(
                    self.ats_name,
                    f"circuit breaker open "
                    f"(>{config.SCRAPE_CIRCUIT_BREAKER_THRESHOLD} consecutive failures)",
                )

            # ── HTTP request (with retry) ────────────────────────────────
            resp = self._request_with_retry(url)
            made_request = True

            if resp.status_code != 200:
                return None  # 404/410/other non-retryable → "not on this ATS"

            # ── Process successful response ──────────────────────────────
            all_jobs = self.extract_all_jobs(resp.json())

            if self.skip_if_board_empty and not all_jobs:
                return None

            jobs = [j for j in all_jobs if self.is_job_relevant(j)]
            scraped_at = datetime.now(timezone.utc).isoformat()

            if jobs:
                # Save filtered jobs to filesystem
                company_dir = os.path.join(output_dir, normalized)
                os.makedirs(company_dir, exist_ok=True)

                with open(os.path.join(company_dir, "jobs.json"), "w") as f:
                    json.dump(self.wrap_for_save(jobs), f, indent=2)

                metadata = {
                    "original_name": company_name,
                    "normalized_name": normalized,
                    "job_count": len(jobs),
                    "ats": self.ats_name,
                    "url": board_url,
                    "scraped_at": scraped_at,
                }
                with open(os.path.join(company_dir, "metadata.json"), "w") as f:
                    json.dump(metadata, f, indent=2)

                if save_to_db:
                    new_count = self._upsert_jobs(
                        company_name, jobs, scraped_at, company_id, db_conn,
                    )
                    metadata["new_job_count"] = new_count
                    self._deactivate_stale_jobs(company_id, scraped_at, db_conn)

                return metadata

            # ATS found but no matching jobs after filtering
            if save_to_db and company_id:
                self._deactivate_stale_jobs(company_id, scraped_at, db_conn)
            return {
                "original_name": company_name,
                "normalized_name": normalized,
                "job_count": 0,
                "total_before_filter": len(all_jobs),
                "ats": self.ats_name,
                "url": board_url,
                "scraped_at": scraped_at,
            }

        except ATSUnavailableError:
            raise  # let callers distinguish transient failures from "not found"

        except Exception as e:
            print(f"  Error scraping {self.ats_name.title()} for {normalized}: {e}")
            return None

        finally:
            # Rate-limit delay only when we actually hit the API.
            # Retry back-off sleeps are handled inside _request_with_retry().
            if made_request:
                time.sleep(delay)

    # ── Database helpers ─────────────────────────────────────────────────

    def _upsert_jobs(
        self,
        company_name: str,
        jobs: list[dict],
        scraped_at: str,
        company_id: int | None,
        db_conn=None,
    ) -> int:
        """Insert new job listings, skip duplicates by job_url.

        Returns the number of newly inserted jobs.
        """
        from db import database

        upsert_sql = """
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
        check_sql = "SELECT 1 FROM job_listings WHERE job_url = ? LIMIT 1"

        new_count = 0
        for job in jobs:
            fields = self.extract_job_fields(job)
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
                scraped_at,  # first_seen_at (only set on initial insert)
                scraped_at,  # last_seen_at (updated on every upsert)
                fields["posted_at"],
                json.dumps(job),
            )

            try:
                if db_conn is not None:
                    exists = db_conn.execute(check_sql, (job_url,)).fetchone()
                    db_conn.execute(upsert_sql, params)
                    if not exists:
                        new_count += 1
                else:
                    with database.get_db() as conn:
                        exists = conn.execute(check_sql, (job_url,)).fetchone()
                        conn.execute(upsert_sql, params)
                        if not exists:
                            new_count += 1
            except Exception as e:
                print(f"    DB error inserting job {job_url}: {e}")

        return new_count

    def _deactivate_stale_jobs(
        self,
        company_id: int | None,
        scraped_at: str,
        db_conn=None,
    ):
        """Mark jobs as inactive if they weren't seen in the latest scrape.

        Any job for this company/ATS whose ``last_seen_at`` is older than
        *scraped_at* is no longer on the board and should be deactivated.
        """
        if not company_id:
            return
        from db import database

        sql = """
            UPDATE job_listings SET is_active = 0
            WHERE company_id = ? AND ats_system = ? AND last_seen_at < ?
        """
        params = (company_id, self.ats_name, scraped_at)
        if db_conn is not None:
            db_conn.execute(sql, params)
        else:
            database.execute(sql, params)
