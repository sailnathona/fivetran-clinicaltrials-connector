#!/usr/bin/env python3
"""
Minimal ClinicalTrials.gov connector (starter)

- Fetches trials from ClinicalTrials.gov v2 API
- Paginates using nextPageToken
- Emits JSON Lines (one record per line) to STDOUT
- Fields match your schema.json (nct_id, official_title, overall_status, phase, conditions, locations, last_changed_date, ingestion_ts)

Usage (examples):
  python connector.py --search 'Alzheimer OR Dementia OR "mild cognitive impairment"' --page-size 100 --max-pages 5
  SEARCH='Alzheimer OR Dementia' python connector.py
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import urllib.parse
import requests


BASE_URL = "https://clinicaltrials.gov/api/v2/studies"


def g(d: Dict[str, Any], path: str, default=None):
    """
    Safe nested getter using dot-separated paths.
    Example: g(study, "protocolSection.identificationModule.nctId")
    """
    cur = d
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def extract_record(study: Dict[str, Any]) -> Dict[str, Any]:
    """Map a raw study JSON into the flat record your schema expects."""
    # Try common v2 locations (robust to missing fields)
    nct_id = g(study, "protocolSection.identificationModule.nctId") or g(study, "identificationModule.nctId")
    official_title = g(study, "protocolSection.identificationModule.officialTitle") or g(
        study, "protocolSection.identificationModule.briefTitle"
    )
    overall_status = g(study, "protocolSection.statusModule.overallStatus")
    phase = g(study, "protocolSection.designModule.phases") or g(study, "protocolSection.designModule.phase")
    conditions = g(study, "protocolSection.conditionsModule.conditions") or []

    # Collect location countries if present
    locations_block = g(study, "protocolSection.contactsLocationsModule.locations") or []
    locations = []
    if isinstance(locations_block, list):
        for loc in locations_block:
            country = loc.get("country")
            if country:
                locations.append({"country": country})

    # Dates: prefer lastUpdatePostDate or lastChangedDate
    last_changed = (
        g(study, "protocolSection.statusModule.lastUpdatePostDate")
        or g(study, "protocolSection.statusModule.lastChangedDate")
        or g(study, "protocolSection.statusModule.lastKnownStatusDate")
    )

    # Normalize phase to a string
    if isinstance(phase, list):
        phase = ", ".join(phase)

    record = {
        "nct_id": nct_id,
        "official_title": official_title,
        "overall_status": overall_status,
        "phase": phase,
        "conditions": conditions,   # JSON column
        "locations": locations,     # JSON column (list of {country})
        "last_changed_date": last_changed,
        "ingestion_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    return record


def build_params(search: str, page_size: int, page_token: Optional[str] = None, start_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Build API params for v2.
    Notes:
      - `query.cond` searches condition terms.
      - You can add other filters later (status, phase, date ranges).
      - `nextPageToken` is used for pagination.
    """
    params = {
        "pageSize": page_size,
        "countTotal": "true",
    }

    # Use condition search for our keywords
    # (If you prefer free-text, swap to 'query.term')
    if search:
        params["query.cond"] = search

    if page_token:
        params["pageToken"] = page_token

    # Optional incremental: filter by last-changed/update date
    # ClinicalTrials.gov v2 supports various filters; adjust as needed once you settle on the field.
    if start_date:
        # Placeholder for date filtering; tune once you decide which exact field to filter on.
        # e.g., params["filter.lastUpdatePostDateFrom"] = start_date
        pass

    return params


def fetch_page(session: requests.Session, params: Dict[str, Any]) -> Dict[str, Any]:
    resp = session.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def run(search: str, page_size: int, max_pages: int, sleep_s: float, start_date: Optional[str]) -> int:
    session = requests.Session()
    page_token = None
    pages = 0
    emitted = 0

    while True:
        if pages >= max_pages:
            break

        params = build_params(search=search, page_size=page_size, page_token=page_token, start_date=start_date)
        data = fetch_page(session, params)

        studies = data.get("studies") or []
        for st in studies:
            rec = extract_record(st)
            # Only emit if we have a primary key
            if rec.get("nct_id"):
                sys.stdout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                emitted += 1

        pages += 1
        page_token = data.get("nextPageToken")
        if not page_token:
            break

        if sleep_s > 0:
            time.sleep(sleep_s)

    return emitted


def parse_args():
    p = argparse.ArgumentParser(description="ClinicalTrials.gov → JSONL (stdout)")
    p.add_argument("--search", default=os.getenv("SEARCH", 'Alzheimer OR Dementia OR "mild cognitive impairment"'),
                   help="Search expression (conditions).")
    p.add_argument("--page-size", type=int, default=int(os.getenv("PAGE_SIZE", "100")),
                   help="API page size (max ~100).")
    p.add_argument("--max-pages", type=int, default=int(os.getenv("MAX_PAGES", "10")),
                   help="Max pages to fetch.")
    p.add_argument("--sleep-s", type=float, default=float(os.getenv("SLEEP_S", "0.2")),
                   help="Delay between pages (seconds).")
    p.add_argument("--start-date", default=os.getenv("START_DATE"),
                   help="Optional ISO date for incremental logic (placeholder).")
    return p.parse_args()


def main():
    args = parse_args()
    emitted = run(
        search=args.search,
        page_size=args.page_size,
        max_pages=args.max_pages,
        sleep_s=args.sleep_s,
        start_date=args.start_date,
    )
    # Exit code 0 even if 0 rows (useful in scheduled runs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

