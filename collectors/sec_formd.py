"""SEC EDGAR Form D data collector.

Downloads quarterly bulk data set ZIPs from SEC EDGAR, parses the
tab-delimited TXT files, and loads company records into SQLite.

For the most recent quarter gap (2026 Q1), falls back to the EFTS
full-text search API which provides filing metadata (no amounts).
"""

import csv
import io
import os
import time
import zipfile

import requests
from tqdm import tqdm

import config
from db import database
from matching.normalize import normalize_company_name


# ─── Bulk Data Set Download ──────────────────────────────────────────────────


def _download_file(url: str, dest_path: str) -> bool:
    """Download a file with progress bar. Skip if already cached.

    Returns:
        True if the file was downloaded or already exists, False on error.
    """
    if os.path.exists(dest_path):
        print(f"  Cached: {os.path.basename(dest_path)}")
        return True

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    headers = {"User-Agent": config.SEC_USER_AGENT}

    try:
        resp = requests.get(url, headers=headers, stream=True, timeout=60)
        resp.raise_for_status()

        total = int(resp.headers.get("content-length", 0))
        with open(dest_path, "wb") as f, tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            desc=os.path.basename(dest_path),
            leave=False,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))

        return True
    except requests.RequestException as e:
        print(f"  Download failed for {url}: {e}")
        # Clean up partial download
        if os.path.exists(dest_path):
            os.remove(dest_path)
        return False


def download_quarterly_zips() -> list[str]:
    """Download all quarterly Form D bulk data ZIPs.

    Returns:
        List of paths to successfully downloaded ZIP files.
    """
    print("Downloading SEC Form D quarterly data sets...")
    downloaded = []
    for q in config.SEC_QUARTERS:
        dest = os.path.join(config.SEC_DATA_DIR, q["filename"])
        if _download_file(q["url"], dest):
            downloaded.append(dest)
        time.sleep(config.SEC_RATE_LIMIT_DELAY)
    print(f"  {len(downloaded)}/{len(config.SEC_QUARTERS)} quarterly files ready.")
    return downloaded


# ─── ZIP Parsing ─────────────────────────────────────────────────────────────


def _read_tsv_from_zip(zf: zipfile.ZipFile, filename_contains: str) -> list[dict]:
    """Read a tab-delimited file from inside a ZIP, return list of dicts."""
    for name in zf.namelist():
        if filename_contains.lower() in name.lower():
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                reader = csv.DictReader(text, delimiter="\t")
                return list(reader)
    return []


def parse_zip(zip_path: str) -> list[dict]:
    """Parse a quarterly ZIP into company records.

    JOINs FORMDSUBMISSION + ISSUERS + OFFERING on ACCESSIONNUMBER.

    Returns:
        List of dicts with keys matching the sec_formd_companies schema.
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            submissions = _read_tsv_from_zip(zf, "SUBMISSION")
            issuers = _read_tsv_from_zip(zf, "ISSUER")
            offerings = _read_tsv_from_zip(zf, "OFFERING")
    except (zipfile.BadZipFile, OSError) as e:
        print(f"  Error reading {zip_path}: {e}")
        return []

    # Index by accession number for joining
    sub_map = {}
    for row in submissions:
        acc = row.get("ACCESSIONNUMBER", "").strip()
        if acc:
            sub_map[acc] = row

    offer_map = {}
    for row in offerings:
        acc = row.get("ACCESSIONNUMBER", "").strip()
        if acc:
            offer_map[acc] = row

    records = []
    for issuer in issuers:
        acc = issuer.get("ACCESSIONNUMBER", "").strip()
        if not acc:
            continue

        company_name = (issuer.get("ENTITYNAME") or "").strip()
        if not company_name:
            continue

        sub = sub_map.get(acc, {})
        offer = offer_map.get(acc, {})

        # Parse total amount sold
        amount_str = (offer.get("TOTALAMOUNTSOLD") or "").strip()
        try:
            total_amount = float(amount_str) if amount_str else None
        except (ValueError, TypeError):
            total_amount = None

        records.append(
            {
                "company_name": company_name,
                "cik_number": (issuer.get("CIK") or "").strip() or None,
                "state": (issuer.get("STATEORCOUNTRY") or "").strip() or None,
                "industry_group": (offer.get("INDUSTRYGROUPTYPE") or "").strip()
                or None,
                "total_amount_sold": total_amount,
                "filing_date": (
                    sub.get("FILING_DATE") or sub.get("FILINGDATE") or ""
                ).strip()
                or None,
                "normalized_name": normalize_company_name(company_name),
            }
        )

    return records


# ─── EFTS API Fallback (2026 Q1 gap) ────────────────────────────────────────


def fetch_recent_efts(
    start_date: str | None = None, end_date: str | None = None
) -> list[dict]:
    """Fetch recent Form D filings via the EFTS search-index API.

    Used to fill the gap between the last quarterly bulk data set and today.
    Returns metadata only (no offering amounts).

    Args:
        start_date: ISO date string (default: config value).
        end_date: ISO date string (default: config value).

    Returns:
        List of dicts matching sec_formd_companies schema.
    """
    start = start_date or config.SEC_EFTS_START_DATE
    end = end_date or config.SEC_EFTS_END_DATE

    print(f"Fetching recent Form D filings via EFTS API ({start} to {end})...")

    headers = {"User-Agent": config.SEC_USER_AGENT}
    records = []
    offset = 0
    page_size = config.SEC_EFTS_PAGE_SIZE

    while True:
        params = {
            "forms": "D",
            "dateRange": "custom",
            "startdt": start,
            "enddt": end,
            "from": offset,
            "size": page_size,
        }

        try:
            resp = requests.get(
                config.SEC_EFTS_BASE_URL,
                params=params,
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError) as e:
            print(f"  EFTS API error at offset {offset}: {e}")
            break

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            source = hit.get("_source", {})
            display_names = source.get("display_names", [])
            ciks = source.get("ciks", [])

            # Each hit may contain multiple issuers; take the first
            company_name = ""
            if display_names:
                # Format: "Company Name (CIK 0001234567)"
                raw = display_names[0]
                company_name = raw.split(" (CIK")[0].strip()

            if not company_name:
                continue

            records.append(
                {
                    "company_name": company_name,
                    "cik_number": ciks[0] if ciks else None,
                    "state": (source.get("biz_states") or [None])[0],
                    "industry_group": None,  # Not available from EFTS
                    "total_amount_sold": None,  # Not available from EFTS
                    "filing_date": source.get("file_date"),
                    "normalized_name": normalize_company_name(company_name),
                }
            )

        total_hits = data.get("hits", {}).get("total", {})
        total_count = (
            total_hits.get("value", 0) if isinstance(total_hits, dict) else total_hits
        )
        offset += page_size

        if offset >= total_count or offset >= 10000:  # ES hard limit
            break

        time.sleep(config.SEC_RATE_LIMIT_DELAY)

    print(f"  Fetched {len(records)} recent filings from EFTS API.")
    return records


# ─── Database Loading ────────────────────────────────────────────────────────


def load_to_db():
    """Download, parse, and load all SEC Form D data into the database."""
    database.init_db()
    database.clear_table("sec_formd_companies")

    all_records = []

    # 1. Quarterly bulk data
    zip_paths = download_quarterly_zips()
    for zp in zip_paths:
        print(f"  Parsing {os.path.basename(zp)}...")
        records = parse_zip(zp)
        all_records.extend(records)
        print(f"    → {len(records)} companies")

    # 2. EFTS fallback for recent filings
    efts_records = fetch_recent_efts()
    all_records.extend(efts_records)

    # 3. Insert into database
    if all_records:
        database.insert_many("sec_formd_companies", all_records)
        print(f"\nLoaded {len(all_records)} total SEC Form D records into database.")
    else:
        print("\nNo SEC Form D records to load.")

    return len(all_records)
