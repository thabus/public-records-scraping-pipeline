"""
King County (Seattle) Property Tax Delinquencies scraper (DEMO — fictional source for portfolio).

Demonstrates:
- Pure REST API scraper — no browser needed
- ArcGIS FeatureServer pagination (exceededTransferLimit loop)
- Epoch-milliseconds → MM/DD/YYYY + YYYY-MM-DD conversion
- Signal handler pattern (registered inside scrape(), restored in finally)
- retry_with_backoff with requests.exceptions
- Date format normalization (accepts both MM/DD/YYYY and YYYY-MM-DD from CLI)
"""

from __future__ import annotations

import signal
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests
from loguru import logger

from common.retry import retry_with_backoff, RetryStats

SUPPORTED_MARKETS = ["seattle_wa"]
COUNTY_NAME = "King County, WA"

# Fictional ArcGIS FeatureServer endpoint
FEATURE_SERVER_URL = (
    "https://services.arcgis.example.com/KingCounty/arcgis/rest/services/"
    "Property_Tax_Delinquencies/FeatureServer/0/query"
)

PAGE_SIZE = 500


# ---------------------------------------------------------------------------
# Private sentinel — raised instead of os._exit on SIGINT/SIGTERM
# ---------------------------------------------------------------------------

class _ScraperInterrupted(Exception):
    pass


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _ms_to_display_date(ms: Optional[int]) -> str:
    """Convert ArcGIS epoch-milliseconds to MM/DD/YYYY (pipeline display field)."""
    if not ms:
        return ""
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%m/%d/%Y")


def _ms_to_iso(ms: Optional[int]) -> str:
    """Convert ArcGIS epoch-milliseconds to YYYY-MM-DD (fetched_at field)."""
    if not ms:
        return datetime.now().strftime("%Y-%m-%d")
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# API layer
# ---------------------------------------------------------------------------

def _build_where_clause(start_date: str, end_date: str) -> str:
    """
    Build ArcGIS SQL WHERE clause.
    Both dates must be in YYYY-MM-DD format at this point.
    """
    return (
        f"(Delinquency_Date >= timestamp '{start_date} 00:00:00' "
        f"AND Delinquency_Date <= timestamp '{end_date} 23:59:59')"
    )


def _fetch_page(where: str, offset: int, session: requests.Session) -> dict:
    """Fetch one page of results from the FeatureServer."""
    params = {
        "f":                 "json",
        "where":             where,
        "outFields":         "*",
        "returnGeometry":    "true",
        "outSR":             "4326",         # WGS84 lat/lon
        "orderByFields":     "Delinquency_Date DESC",
        "resultOffset":      offset,
        "resultRecordCount": PAGE_SIZE,
    }
    resp = session.get(FEATURE_SERVER_URL, params=params, timeout=90)
    resp.raise_for_status()
    return resp.json()


def _fetch_all(where: str) -> List[dict]:
    """
    Paginate through all matching FeatureServer records.
    Loops until `exceededTransferLimit` is absent or False.
    """
    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    all_features: List[dict] = []
    offset = 0

    while True:
        data = _fetch_page(where, offset, session)

        if "error" in data:
            raise RuntimeError(f"ArcGIS API error: {data['error']}")

        features = data.get("features", [])
        all_features.extend(features)
        logger.debug(f"  offset={offset}: {len(features)} records")

        if not data.get("exceededTransferLimit", False) or not features:
            break

        offset += PAGE_SIZE

    return all_features


# ---------------------------------------------------------------------------
# Result builder
# ---------------------------------------------------------------------------

def _build_result(feat: dict, market_slug: str) -> Dict:
    attrs = feat.get("attributes", {})
    geom  = feat.get("geometry", {})

    parcel_id   = attrs.get("Parcel_ID", "")
    owner_name  = attrs.get("Owner_Name", "")
    amount_due  = attrs.get("Amount_Due", "")
    tax_year    = attrs.get("Tax_Year", "")
    address     = attrs.get("Situs_Address", "")
    delinq_ms   = attrs.get("Delinquency_Date")
    legal_desc  = attrs.get("Legal_Description", "")

    latitude    = geom.get("y", "")
    longitude   = geom.get("x", "")

    file_date  = _ms_to_display_date(delinq_ms)
    fetched_at = _ms_to_iso(delinq_ms)

    # Parse owner name: King County stores as "LAST FIRST" (space-separated)
    parts = owner_name.strip().split()
    if len(parts) >= 2:
        last_name  = parts[0]
        first_name = " ".join(parts[1:])
    else:
        last_name  = owner_name.strip()
        first_name = ""

    legal_raw = f"{legal_desc} | Tax Year: {tax_year} | Amount Due: ${amount_due}"
    title     = f"{parcel_id}-{file_date}-{FEATURE_SERVER_URL}"

    return {
        "first_name":     first_name,
        "last_name":      last_name,
        "legal_raw":      legal_raw,
        "doc_or_case_id": parcel_id,
        "link":           "https://blue.kingcounty.example.gov/Assessor/",
        "source":         "king_county_tax_delinquency",
        "file_date":      file_date,
        "market":         market_slug,
        "title":          title,
        "source_url":     FEATURE_SERVER_URL,
        "fetched_at":     fetched_at,
        "document_type":  f"KINGCO_TAX:{tax_year}:{parcel_id}",
        "latitude":       latitude,
        "longitude":      longitude,
        "parcel_address": address,
        "amount_due":     amount_due,
        "tax_year":       tax_year,
    }


# ---------------------------------------------------------------------------
# Core scraping logic
# ---------------------------------------------------------------------------

def _scrape_logic(start_date: str, end_date: str, market_slug: str) -> List[Dict]:
    """Fetch all delinquency records in the date range and build result dicts."""
    where = _build_where_clause(start_date, end_date)
    logger.info(f"King County Tax Delinquencies: {start_date} → {end_date}")
    logger.debug(f"WHERE: {where}")

    features = _fetch_all(where)
    logger.info(f"Total features fetched: {len(features)}")

    results = []
    for feat in features:
        result = _build_result(feat, market_slug)
        results.append(result)

    return results


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def scrape(
    market: str = "seattle_wa",
    retry_stats: RetryStats = None,
    **kwargs,
) -> List[Dict]:
    if market not in SUPPORTED_MARKETS:
        raise ValueError(
            f"Scraper 'king_county_api' is specific to {COUNTY_NAME}. "
            f"Market '{market}' is not supported. "
            f"Supported markets: {', '.join(SUPPORTED_MARKETS)}"
        )

    today      = datetime.now()
    start_date = kwargs.get("start_date") or (today - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date   = kwargs.get("end_date")   or today.strftime("%Y-%m-%d")

    # Normalize CLI input — accept both MM/DD/YYYY and YYYY-MM-DD
    for fmt in ("%m/%d/%Y",):
        try:
            start_date = datetime.strptime(start_date, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
        try:
            end_date = datetime.strptime(end_date, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # --- Signal handling (registered here, not at module level) --------------
    _interrupted = False
    _original_sigint  = signal.getsignal(signal.SIGINT)
    _original_sigterm = signal.getsignal(signal.SIGTERM)

    def _handle_signal(signum, frame):
        nonlocal _interrupted
        logger.warning(f"Signal {signum} received — stopping King County scraper gracefully")
        _interrupted = True
        raise _ScraperInterrupted()

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    def _run():
        if _interrupted:
            raise _ScraperInterrupted()
        return _scrape_logic(start_date, end_date, market)

    try:
        return retry_with_backoff(
            _run,
            retryable_exceptions=(
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ),
            stats=retry_stats,
        )
    except _ScraperInterrupted:
        logger.info("King County scraper interrupted — returning partial results")
        return []
    except Exception as e:
        traceback.print_exc()
        logger.error(f"King County API scraper failed after retries: {e}")
        return []
    finally:
        signal.signal(signal.SIGINT,  _original_sigint)
        signal.signal(signal.SIGTERM, _original_sigterm)


__all__ = ["scrape"]
