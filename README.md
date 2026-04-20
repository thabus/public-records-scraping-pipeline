# Real Estate Data Scraping Pipeline

A production Python scraping pipeline that collects real estate and legal records (deed filings, court cases, code enforcement, tax liens, probate records) from county clerk portals, court systems, and open government APIs across multiple US markets.

Built and maintained as a freelance contractor. The pipeline feeds a downstream lead-generation database used by real estate investors.

---

## Architecture overview

```
scraping/
├── bin/
│   └── scrape.py          # CLI entry point — validates args, routes to scrapers, writes output
├── scrapers/
│   ├── __init__.py        # Registers all scraper modules
│   ├── util.py            # Shared browser classes, helpers, date utils
│   ├── county_clerk_a.py  # One file per source
│   ├── county_clerk_b.py
│   └── ...
database/
└── common/
    └── config.py          # Per-market defaults (counties, date ranges, etc.)
scripts/
└── crontab.production     # Cron schedule for all scrapers
```

Each scraper is a self-contained module that exposes a single `scrape(market, retry_stats, **kwargs) -> List[Dict]` function. The pipeline handles CSV output, deduplication, schema validation, and routing — scrapers only return data.

---

## Tech stack

| Layer | Tools |
|---|---|
| Browser automation | Playwright, Patchright (anti-bot), playwright-stealth |
| HTTP | requests, BrightData residential/datacenter proxies |
| Parsing | BeautifulSoup4, regex |
| Logging | loguru |
| Scheduling | Linux cron + Xvfb (virtual display for headless Chrome) |
| Retry logic | Custom `retry_with_backoff` with per-exception config |
| Data validation | Custom schema validator, CSV dedup layer |

---

## Key engineering challenges solved

### 1. Anti-bot detection bypass
Several county portals use Cloudflare or custom bot detection. Solution: `UndetectedBrowser` wraps **Patchright** (a Chromium fork that removes the `Runtime.enable` CDP command — the main fingerprint vector) with `headless=False` + Xvfb on the server, random mouse movements, human-like typing delays, and user-agent rotation.

### 2. Angular / jQuery form automation
Some portals use Angular or jQuery for form inputs. Direct `page.fill()` calls don't trigger Angular's change detection. Solution: inject `dispatchEvent('input')` via `page.evaluate()` to fire framework watchers. For heavy DOM pages, `document.getElementById().click()` via JS avoids CBOR stack overflows from Playwright's own locator engine.

### 3. Resilient retry with clean signal handling
All scrapers register a `SIGINT`/`SIGTERM` handler **inside** `scrape()` (never at module level, to avoid polluting Prefect's process) and raise a private `_ScraperInterrupted` exception instead of calling `os._exit`. The original signal handler is always restored in the `finally` block alongside browser cleanup.

### 4. Paginated REST APIs
Some sources expose ArcGIS FeatureServer or similar REST endpoints. Scrapers detect `exceededTransferLimit` in the response and loop with `resultOffset` until all pages are consumed. Date fields arrive as epoch-milliseconds and are normalized to both `MM/DD/YYYY` (pipeline field) and `YYYY-MM-DD` (`fetched_at`).

### 5. Date format normalization
CLI accepts both `MM/DD/YYYY` and `YYYY-MM-DD`. Each scraper converts to its internal format with a `try/except` chain so either format works without breaking existing cron jobs.

---

## Scraper pattern

Every module follows the same contract:

```python
SUPPORTED_MARKETS = ["market_slug"]

def scrape(market: str, retry_stats: RetryStats = None, **kwargs) -> List[Dict]:
    # 1. Validate market
    # 2. Parse date range from kwargs (with sensible defaults)
    # 3. Run core logic inside retry_with_backoff
    # 4. Clean up browser in finally block
    # 5. Return List[Dict] — one dict per record
```

Required output fields per record:

```
first_name, last_name, legal_raw, doc_or_case_id,
link, source, file_date (MM/DD/YYYY), market, title,
source_url, fetched_at (YYYY-MM-DD), document_type
```

---

## Markets covered (examples)

| Slug | Area | Source type |
|---|---|---|
| `hickory_nc` | Catawba County, NC | Clerk portal (browser) |
| `cobb_ga` | Atlanta metro (Cobb Co.) | Clerk portal (browser + jQuery) |
| `virginia_beach_va` | Virginia Beach, VA | State court system (browser + JS eval) |
| `nashville_tn` | Davidson County, TN | Court system + ArcGIS API |
| `marion_in` | Indianapolis metro | MyCase court portal (browser) |
| `salt_lake_city_ut` | Salt Lake City, UT | State tax lien + court calendar |

---

## Running a scraper locally

```bash
# Single scraper, last 30 days
python scraping/bin/scrape.py --scraper cobb_county --market cobb_ga

# With explicit date range
python scraping/bin/scrape.py --scraper vb_circuit \
    --market virginia_beach_va \
    --start-date 01/01/2025 \
    --end-date 01/31/2025

# Output goes to data/output/<scraper>_<market>_<date>.csv
```

---

## Demo scrapers (this repo)

The two scrapers in this repository are **fictional demos** written specifically for this portfolio. They are not connected to any real client system. They demonstrate the same patterns, browser classes, and conventions used in production:

- [`miami_dade_liens.py`](scrapers/miami_dade_liens.py) — browser automation with anti-bot bypass (Patchright + stealth), JS form interaction, BeautifulSoup parsing, pagination
- [`king_county_api.py`](scrapers/king_county_api.py) — pure REST API scraper, ArcGIS-style pagination, epoch-ms date handling, no browser needed

---

## Code conventions enforced across all scrapers

- No unused imports
- Trailing newline at end of every file
- `fetched_at` never empty — always falls back to `datetime.now().strftime("%Y-%m-%d")`
- Signal handler registered inside `scrape()`, not at module level
- Browser initialized outside retry lambda to avoid asyncio conflicts with Prefect
- `import csv` only if actually used
