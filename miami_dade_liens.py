"""
Miami-Dade County Liens scraper (DEMO — fictional source for portfolio).

Demonstrates:
- UndetectedBrowser (Patchright) for anti-bot bypass
- JS-based form interaction (dispatchEvent, getElementById.click)
- BeautifulSoup parsing with multi-page pagination
- Signal handler pattern (registered inside scrape(), restored in finally)
- retry_with_backoff with PlaywrightTimeoutError / ConnectionError
"""

from __future__ import annotations

import re
import signal
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from loguru import logger
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError

from common.retry import retry_with_backoff, RetryStats
from scraping.scrapers.util import UndetectedBrowser, convert_string_date_to_iso_format

SUPPORTED_MARKETS = ["miami_fl"]
COUNTY_NAME = "Miami-Dade County, FL"
SCRAPER_URL = "https://www.miamidade.example.gov"  # fictional


# ---------------------------------------------------------------------------
# Private sentinel — raised instead of os._exit on SIGINT/SIGTERM
# ---------------------------------------------------------------------------

class _ScraperInterrupted(Exception):
    pass


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_name(raw: str) -> tuple[str, str]:
    """
    Parse 'LAST, FIRST [MIDDLE]' format common in county clerk portals.
    Returns (first_name, last_name).
    """
    if not raw or not raw.strip():
        return ("", "")
    if "," in raw:
        last, rest = raw.split(",", 1)
        first = rest.strip().split()[0] if rest.strip() else ""
        return (first.strip(), last.strip())
    parts = raw.strip().split()
    if len(parts) < 2:
        return ("", parts[0])
    return (parts[-1], " ".join(parts[:-1]))


def _extract_apn(legal_raw: str) -> str:
    """
    Extract Miami-Dade APN from legal description.
    Format: XX-XXXX-XXX-XXXX (fictional pattern).
    """
    if not legal_raw:
        return ""
    match = re.search(r"\b\d{2}-\d{4}-\d{3}-\d{4}\b", legal_raw)
    return match.group(0) if match else ""


# ---------------------------------------------------------------------------
# Page-level scraping
# ---------------------------------------------------------------------------

def _scrape_page(
    browser: UndetectedBrowser,
    source: str,
    market_slug: str,
    page_num: int,
) -> List[Dict]:
    """Parse one result page and return a list of records."""
    html = browser.get_actual_html()
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table", {"id": "lienResultsTable"})
    if not table:
        logger.warning(f"lienResultsTable not found on page {page_num}")
        return []

    rows = table.find("tbody").find_all("tr")
    logger.info(f"  Page {page_num}: {len(rows)} rows")

    results: List[Dict] = []
    for idx, tr in enumerate(rows, 1):
        tds = tr.find_all("td")
        if len(tds) < 7:
            logger.debug(f"    Row {idx}: skipped (only {len(tds)} columns)")
            continue

        doc_or_case_id = tds[0].text.strip()
        raw_name       = tds[1].text.strip()
        file_date_raw  = tds[2].text.strip()       # MM/DD/YYYY from source
        legal_raw      = tds[5].text.strip()
        doc_type_raw   = tds[6].text.strip()

        first_name, last_name = _parse_name(raw_name)

        try:
            fetched_at = convert_string_date_to_iso_format(file_date_raw, "%m/%d/%Y")
        except ValueError:
            fetched_at = datetime.now().strftime("%Y-%m-%d")

        link = browser.get_url()
        title = f"{doc_or_case_id}-{file_date_raw}-{link}"

        results.append({
            "first_name":     first_name,
            "last_name":      last_name,
            "legal_raw":      legal_raw,
            "apn":            _extract_apn(legal_raw),
            "doc_or_case_id": doc_or_case_id,
            "link":           link,
            "source":         source,
            "file_date":      file_date_raw,
            "market":         market_slug,
            "title":          title,
            "source_url":     f"{SCRAPER_URL}/liens/search",
            "fetched_at":     fetched_at,
            "document_type":  f"MIAMIFL:{doc_type_raw}:{doc_or_case_id}",
        })

        logger.info(f"    ✅ Row {idx}: {doc_or_case_id} — {last_name}, {first_name}")

    return results


def _run_search(
    browser: UndetectedBrowser,
    start_date: str,
    end_date: str,
    source: str,
    market_slug: str,
) -> List[Dict]:
    """
    Navigate to the search form, submit, then walk all result pages.

    Anti-bot notes:
    - dispatchEvent('input') is required to trigger the Angular change-detection
      on the date inputs — page.fill() alone silently fails.
    - document.getElementById(...).click() avoids CBOR stack overflow on the
      heavy results table rendered by this portal's DataTables setup.
    """
    browser.get_html(f"{SCRAPER_URL}/liens/search", wait=2)

    # --- Fill date inputs via JS (Angular portal) ----------------------------
    browser.execute_js(
        f"""
        (function() {{
            var el = document.getElementById('startDate');
            el.value = '{start_date}';
            el.dispatchEvent(new Event('input', {{ bubbles: true }}));
            el.dispatchEvent(new Event('change', {{ bubbles: true }}));
        }})()
        """
    )
    browser.execute_js(
        f"""
        (function() {{
            var el = document.getElementById('endDate');
            el.value = '{end_date}';
            el.dispatchEvent(new Event('input', {{ bubbles: true }}));
            el.dispatchEvent(new Event('change', {{ bubbles: true }}));
        }})()
        """
    )

    # Filter by document types relevant to the pipeline
    browser.execute_js(
        "(() => document.getElementById('docTypeFilter').value = 'LIEN,TAXLIEN,MTGLIEN')()"
    )

    # Submit — use JS click to avoid CBOR overflow on heavy DOM
    browser.execute_js(
        "(() => document.getElementById('searchSubmitBtn').click())()",
        wait_time=4,
    )

    all_results: List[Dict] = []
    page_num = 1

    while True:
        page_results = _scrape_page(browser, source, market_slug, page_num)
        all_results.extend(page_results)

        # Check for "Next" button
        has_next: Optional[bool] = browser.execute_js(
            """
            (() => {
                var btn = document.getElementById('nextPageBtn');
                return btn && !btn.disabled && !btn.classList.contains('disabled');
            })()
            """
        )
        if not has_next:
            break

        browser.execute_js(
            "(() => document.getElementById('nextPageBtn').click())()",
            wait_time=3,
        )
        page_num += 1

    logger.info(f"Total records collected: {len(all_results)}")
    return all_results


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def scrape(
    market: str = "miami_fl",
    retry_stats: RetryStats = None,
    **kwargs,
) -> List[Dict]:
    if market not in SUPPORTED_MARKETS:
        raise ValueError(
            f"Scraper 'miami_dade_liens' is specific to {COUNTY_NAME}. "
            f"Market '{market}' is not supported. "
            f"Supported markets: {', '.join(SUPPORTED_MARKETS)}"
        )

    today = datetime.now()
    start_date = kwargs.get("start_date") or (today - timedelta(days=30)).strftime("%m/%d/%Y")
    end_date   = kwargs.get("end_date")   or today.strftime("%m/%d/%Y")

    # Accept YYYY-MM-DD from CLI as well
    for fmt in ("%Y-%m-%d",):
        try:
            start_date = datetime.strptime(start_date, fmt).strftime("%m/%d/%Y")
        except ValueError:
            pass
        try:
            end_date = datetime.strptime(end_date, fmt).strftime("%m/%d/%Y")
        except ValueError:
            pass

    # UndetectedBrowser (Patchright) is required — this portal uses Cloudflare
    browser = UndetectedBrowser(headless=False)

    # --- Signal handling (registered here, not at module level) --------------
    _interrupted = False
    _original_sigint  = signal.getsignal(signal.SIGINT)
    _original_sigterm = signal.getsignal(signal.SIGTERM)

    def _handle_signal(signum, frame):
        nonlocal _interrupted
        logger.warning(f"Signal {signum} received — stopping Miami-Dade scraper gracefully")
        _interrupted = True
        raise _ScraperInterrupted()

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    def _scrape_task():
        if _interrupted:
            raise _ScraperInterrupted()
        return _run_search(browser, start_date, end_date, "miami_fl", market)

    try:
        return retry_with_backoff(
            _scrape_task,
            retryable_exceptions=(PlaywrightTimeoutError, PlaywrightError, ConnectionError),
            stats=retry_stats,
        )
    except _ScraperInterrupted:
        logger.info("Miami-Dade scraper interrupted — returning partial results")
        return []
    except Exception as e:
        traceback.print_exc()
        logger.error(f"Miami-Dade liens scraper failed after retries: {e}")
        return []
    finally:
        browser.stop()
        signal.signal(signal.SIGINT,  _original_sigint)
        signal.signal(signal.SIGTERM, _original_sigterm)


__all__ = ["scrape"]
