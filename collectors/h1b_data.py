"""H1B / LCA employer data collector.

Downloads and parses:
  - DOL LCA Disclosure Data (Excel .xlsx files)
  - USCIS H1B Employer Data Hub (CSV files)

Aggregates employer records and loads into the h1b_sponsors SQLite table.
"""

import os
import time
from collections import defaultdict

import pandas as pd
import requests
from tqdm import tqdm

import config
from db import database
from matching.normalize import normalize_company_name


# ─── File Download ───────────────────────────────────────────────────────────


def _download_file(url: str, dest_path: str, description: str = "") -> bool:
    """Download a file with progress bar. Skip if already cached.

    Returns:
        True if the file exists (downloaded or cached), False on error.
    """
    if os.path.exists(dest_path):
        print(f"  Cached: {os.path.basename(dest_path)}")
        return True

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    desc = description or os.path.basename(dest_path)

    try:
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        total = int(resp.headers.get("content-length", 0))
        with open(dest_path, "wb") as f, tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            desc=desc,
            leave=False,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
                bar.update(len(chunk))

        return True
    except requests.RequestException as e:
        print(f"  Download failed for {url}: {e}")
        if os.path.exists(dest_path):
            os.remove(dest_path)
        return False


# ─── DOL LCA Disclosure Data ────────────────────────────────────────────────


def download_lca_data() -> list[str]:
    """Download DOL LCA disclosure Excel files.

    Returns:
        List of paths to successfully downloaded files.
    """
    print("Downloading DOL LCA Disclosure Data...")
    downloaded = []
    for fy, info in config.DOL_LCA_FILES.items():
        dest = os.path.join(config.H1B_DATA_DIR, info["filename"])
        if _download_file(info["url"], dest, f"LCA {fy}"):
            downloaded.append(dest)
        time.sleep(0.5)
    print(f"  {len(downloaded)}/{len(config.DOL_LCA_FILES)} LCA files ready.")
    return downloaded


def parse_lca_excel(path: str) -> list[dict]:
    """Parse a DOL LCA disclosure Excel file.

    Filters to certified H-1B applications and aggregates by employer.

    Args:
        path: Path to the .xlsx file.

    Returns:
        List of aggregated employer dicts.
    """
    print(f"  Parsing {os.path.basename(path)} (this may take a minute)...")

    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print(f"  Error reading {path}: {e}")
        return []

    # Normalize column names (DOL files sometimes have inconsistent casing)
    df.columns = [c.strip().upper() for c in df.columns]

    # Filter to certified H-1B cases
    status_col = None
    for candidate in ["CASE_STATUS", "STATUS"]:
        if candidate in df.columns:
            status_col = candidate
            break

    visa_col = None
    for candidate in ["VISA_CLASS", "VISA_TYPE"]:
        if candidate in df.columns:
            visa_col = candidate
            break

    if status_col:
        df = df[df[status_col].str.upper().str.contains("CERTIFIED", na=False)]
    if visa_col:
        df = df[df[visa_col].str.upper().str.contains("H-1B", na=False)]

    # Find employer name column
    employer_col = None
    for candidate in ["EMPLOYER_NAME", "EMPLOYER_BUSINESS_NAME", "NAME"]:
        if candidate in df.columns:
            employer_col = candidate
            break

    if not employer_col:
        print(f"  Could not find employer name column in {path}")
        return []

    # Aggregate by employer
    employer_groups = defaultdict(
        lambda: {
            "count": 0,
            "city": None,
            "state": None,
            "naics_code": None,
        }
    )

    city_col = "EMPLOYER_CITY" if "EMPLOYER_CITY" in df.columns else None
    state_col = "EMPLOYER_STATE" if "EMPLOYER_STATE" in df.columns else None
    naics_col = "NAICS_CODE" if "NAICS_CODE" in df.columns else None

    for _, row in df.iterrows():
        name = str(row.get(employer_col, "")).strip()
        if not name or name == "nan":
            continue

        key = name.upper()
        employer_groups[key]["count"] += 1
        if not employer_groups[key]["city"] and city_col:
            employer_groups[key]["city"] = str(row.get(city_col, "")).strip() or None
        if not employer_groups[key]["state"] and state_col:
            employer_groups[key]["state"] = str(row.get(state_col, "")).strip() or None
        if not employer_groups[key]["naics_code"] and naics_col:
            val = row.get(naics_col)
            if pd.notna(val):
                employer_groups[key]["naics_code"] = (
                    str(int(val)) if isinstance(val, float) else str(val).strip()
                )

    # Derive fiscal year from filename
    fy = "Unknown"
    basename = os.path.basename(path)
    for year_str in ["2025", "2024", "2023"]:
        if year_str in basename:
            fy = f"FY{year_str}"
            break

    records = []
    for employer_name_upper, agg in employer_groups.items():
        # Use the original casing from the first occurrence
        records.append(
            {
                "employer_name": employer_name_upper.title(),  # Title-case for readability
                "city": agg["city"],
                "state": agg["state"],
                "naics_code": agg["naics_code"],
                "visa_class": "H-1B",
                "initial_approvals": agg["count"],  # LCA certifications as proxy
                "continuing_approvals": 0,
                "initial_denials": 0,
                "fiscal_year": fy,
                "normalized_name": normalize_company_name(employer_name_upper),
            }
        )

    print(f"    → {len(records)} unique employers from LCA data")
    return records


# ─── USCIS H1B Employer Data Hub ────────────────────────────────────────────


def download_uscis_data() -> list[str]:
    """Download USCIS H1B employer data CSVs and find local Tableau exports.

    Tries direct CSV URLs (may 404 for FY2024+), then checks for
    manually-downloaded Tableau exports in the H1B data directory.

    Returns:
        List of paths to successfully available files.
    """
    print("Downloading USCIS H1B Employer Data...")
    downloaded = []

    # 1. Try direct CSV downloads
    for fy, info in config.USCIS_FILES.items():
        dest = os.path.join(config.H1B_DATA_DIR, info["filename"])
        if _download_file(info["url"], dest, f"USCIS {fy}"):
            downloaded.append(dest)
        time.sleep(0.5)

    # 2. Pick up manually-downloaded Tableau exports
    for filename in config.USCIS_LOCAL_FILES:
        path = os.path.join(config.H1B_DATA_DIR, filename)
        if os.path.exists(path):
            if path not in downloaded:
                print(f"  Found local: {filename}")
                downloaded.append(path)

    print(f"  {len(downloaded)} USCIS files ready.")
    return downloaded


def parse_uscis_csv(path: str) -> list[dict]:
    """Parse a USCIS H1B employer data CSV.

    Aggregates approval/denial counts per employer.

    Args:
        path: Path to the CSV file.

    Returns:
        List of employer dicts.
    """
    print(f"  Parsing {os.path.basename(path)}...")

    # Try multiple encodings and delimiters (Tableau exports are UTF-16 + tab)
    df = None
    for encoding in ["utf-8", "utf-16", "latin-1"]:
        for sep in [",", "\t"]:
            try:
                df = pd.read_csv(path, encoding=encoding, sep=sep, low_memory=False)
                # Check if we got a reasonable number of columns (not a single-column mess)
                if len(df.columns) >= 5:
                    break
                df = None
            except Exception:
                df = None
        if df is not None:
            break

    if df is None:
        print(f"  Error: could not read {path} with any encoding/delimiter")
        return []

    # Normalize column names — strip whitespace
    df.columns = [c.strip() for c in df.columns]

    # Find key columns — handles both direct CSV and Tableau export formats:
    #   Direct CSV:   "Employer", "City", "State", "NAICS"
    #   Tableau:      "Employer (Petitioner) Name", "Petitioner City", "Petitioner State", "Industry (NAICS) Code"
    col_map = {}
    for col in df.columns:
        cl = col.lower().strip()
        if (
            cl == "employer"
            or "employer" in cl
            and ("name" in cl or "petitioner" in cl)
        ):
            col_map["employer_name"] = col
        elif cl in ("city",) or "city" in cl:
            col_map["city"] = col
        elif cl in ("state",) or ("state" in cl and "line" not in cl):
            col_map["state"] = col
        elif "zip" in cl:
            col_map["zip"] = col
        elif "naics" in cl:
            col_map["naics"] = col
        elif "fiscal" in cl and "year" in cl:
            col_map["fiscal_year"] = col
        elif "new employment" in cl and "approv" in cl:
            col_map["initial_approvals"] = col
        elif "new employment" in cl and "deni" in cl:
            col_map["initial_denials"] = col
        elif "continu" in cl and "approv" in cl:
            col_map["continuing_approvals"] = col
        # Fallback for the FY2023 format ("Initial Approval" / "Initial Denial")
        elif "initial" in cl and "approv" in cl and "initial_approvals" not in col_map:
            col_map["initial_approvals"] = col
        elif "initial" in cl and "deni" in cl and "initial_denials" not in col_map:
            col_map["initial_denials"] = col

    employer_col = col_map.get("employer_name")
    if not employer_col:
        print(f"  Could not find employer name column in {path}")
        return []

    # Aggregate by employer (there may be multiple rows per employer)
    employer_agg = defaultdict(
        lambda: {
            "initial_approvals": 0,
            "continuing_approvals": 0,
            "initial_denials": 0,
            "city": None,
            "state": None,
            "naics_code": None,
            "fiscal_year": None,
        }
    )

    for _, row in df.iterrows():
        name = str(row.get(employer_col, "")).strip()
        if not name or name == "nan":
            continue

        key = name.upper()
        agg = employer_agg[key]

        # Sum approval/denial counts
        for field, csv_col in [
            ("initial_approvals", col_map.get("initial_approvals")),
            ("continuing_approvals", col_map.get("continuing_approvals")),
            ("initial_denials", col_map.get("initial_denials")),
        ]:
            if csv_col:
                val = row.get(csv_col, 0)
                try:
                    agg[field] += int(val) if pd.notna(val) else 0
                except (ValueError, TypeError):
                    pass

        # Take first non-null values for metadata
        if not agg["city"] and col_map.get("city"):
            val = str(row.get(col_map["city"], "")).strip()
            if val and val != "nan":
                agg["city"] = val
        if not agg["state"] and col_map.get("state"):
            val = str(row.get(col_map["state"], "")).strip()
            if val and val != "nan":
                agg["state"] = val
        if not agg["naics_code"] and col_map.get("naics"):
            val = row.get(col_map["naics"])
            if pd.notna(val):
                agg["naics_code"] = (
                    str(int(val)) if isinstance(val, float) else str(val).strip()
                )
        if not agg["fiscal_year"] and col_map.get("fiscal_year"):
            val = row.get(col_map["fiscal_year"])
            if pd.notna(val):
                agg["fiscal_year"] = (
                    f"FY{int(val)}"
                    if isinstance(val, (int, float))
                    else str(val).strip()
                )

    # Derive fiscal year from filename if not in data
    default_fy = "Unknown"
    basename = os.path.basename(path)
    for year_str in ["2025", "2024", "2023"]:
        if year_str in basename:
            default_fy = f"FY{year_str}"
            break

    records = []
    for employer_upper, agg in employer_agg.items():
        records.append(
            {
                "employer_name": employer_upper.title(),
                "city": agg["city"],
                "state": agg["state"],
                "naics_code": agg["naics_code"],
                "visa_class": "H-1B",
                "initial_approvals": agg["initial_approvals"],
                "continuing_approvals": agg["continuing_approvals"],
                "initial_denials": agg["initial_denials"],
                "fiscal_year": agg["fiscal_year"] or default_fy,
                "normalized_name": normalize_company_name(employer_upper),
            }
        )

    print(f"    → {len(records)} unique employers from USCIS data")
    return records


# ─── Merge & Load ────────────────────────────────────────────────────────────


def _merge_employer_records(
    lca_records: list[dict], uscis_records: list[dict]
) -> list[dict]:
    """Merge LCA and USCIS records, preferring USCIS approval/denial counts.

    When the same employer appears in both sources, we keep USCIS
    approval/denial counts (actual petition outcomes) and add LCA
    certified count as a supplemental signal.

    Args:
        lca_records: Records from DOL LCA data.
        uscis_records: Records from USCIS data.

    Returns:
        Merged list of unique employer records.
    """
    # Index USCIS records by normalized name
    uscis_by_name = {}
    for rec in uscis_records:
        key = rec["normalized_name"]
        if key in uscis_by_name:
            # Merge counts for same employer across fiscal years
            existing = uscis_by_name[key]
            existing["initial_approvals"] += rec["initial_approvals"]
            existing["continuing_approvals"] += rec["continuing_approvals"]
            existing["initial_denials"] += rec["initial_denials"]
            # Keep the more recent fiscal year
            if rec.get("fiscal_year", "") > existing.get("fiscal_year", ""):
                existing["fiscal_year"] = rec["fiscal_year"]
        else:
            uscis_by_name[key] = rec.copy()

    # Add LCA records that aren't in USCIS
    merged = dict(uscis_by_name)  # Start with all USCIS records
    for rec in lca_records:
        key = rec["normalized_name"]
        if key not in merged:
            merged[key] = rec.copy()
        else:
            # Employer exists in USCIS data — keep USCIS counts, update metadata if missing
            existing = merged[key]
            if not existing.get("city") and rec.get("city"):
                existing["city"] = rec["city"]
            if not existing.get("state") and rec.get("state"):
                existing["state"] = rec["state"]
            if not existing.get("naics_code") and rec.get("naics_code"):
                existing["naics_code"] = rec["naics_code"]

    return list(merged.values())


def load_to_db():
    """Download, parse, and load all H1B employer data into the database."""
    database.init_db()
    database.clear_table("h1b_sponsors")

    lca_records = []
    uscis_records = []

    # 1. DOL LCA data
    lca_paths = download_lca_data()
    for path in lca_paths:
        records = parse_lca_excel(path)
        lca_records.extend(records)

    # 2. USCIS data
    uscis_paths = download_uscis_data()
    for path in uscis_paths:
        records = parse_uscis_csv(path)
        uscis_records.extend(records)

    # 3. Merge and deduplicate
    print("Merging LCA and USCIS employer data...")
    merged = _merge_employer_records(lca_records, uscis_records)

    # 4. Insert into database
    if merged:
        database.insert_many("h1b_sponsors", merged)
        print(f"\nLoaded {len(merged)} unique H1B sponsor records into database.")
    else:
        print("\nNo H1B sponsor records to load.")

    return len(merged)
