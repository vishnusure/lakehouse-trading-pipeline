"""
Stooq Local Ingestion Script — Phase A
=======================================
Runs LOCALLY on a Mac (not in Databricks). Stooq blocks/rate-limits cloud
IP ranges, so we download from a residential IP, save CSVs to /tmp, then
upload to a Unity Catalog Volume via the Databricks SDK.

Usage:
    python scripts/ingest_stooq_local.py
    python scripts/ingest_stooq_local.py --start 2024-01-01 --end 2024-12-31

Prereqs:
    pip install requests python-dotenv databricks-sdk
    ~/.databrickscfg configured (e.g. via `databricks configure`)
    config/tickers.csv with up to 7 tickers
    config/.env with STOOQ_API_KEY=...
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_TICKERS = 7
REQUEST_TIMEOUT_SECONDS = 10
SLEEP_BETWEEN_REQUESTS_SECONDS = 2
DEFAULT_LOOKBACK_DAYS = 90

LOCAL_TMP_DIR = Path("/tmp/stooq")
UC_VOLUME_PATH = "/Volumes/workspace/bronze/stooq_raw_volume"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TICKERS_CSV = PROJECT_ROOT / "config" / "tickers.csv"
ENV_FILE = PROJECT_ROOT / "config" / ".env"

STOOQ_URL_TEMPLATE = (
    "https://stooq.com/q/d/l/"
    "?s={ticker}&d1={start}&d2={end}&i=d&apikey={key}"
)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("stooq_ingest")


# ---------------------------------------------------------------------------
# A1. Ticker parameterisation
# ---------------------------------------------------------------------------
def load_tickers(path: Path) -> list[str]:
    """Read tickers from a single-column CSV. Strip, uppercase, dedupe order-preserving.
    Enforce hard cap of MAX_TICKERS."""
    if not path.exists():
        raise FileNotFoundError(f"Tickers file not found: {path}")

    tickers: list[str] = []
    seen: set[str] = set()

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row_num, row in enumerate(reader, start=1):
            if not row:
                continue
            value = row[0].strip().upper()
            if not value:
                continue
            # Skip header row if present
            if row_num == 1 and value == "TICKER":
                continue
            if value in seen:
                log.warning("Duplicate ticker skipped: %s", value)
                continue
            seen.add(value)
            tickers.append(value)

    if not tickers:
        raise ValueError(f"No tickers found in {path}")

    if len(tickers) > MAX_TICKERS:
        raise ValueError(
            f"Too many tickers: {len(tickers)} found, max allowed is {MAX_TICKERS}. "
            f"Trim {path} before re-running."
        )

    log.info("Loaded %d ticker(s): %s", len(tickers), ", ".join(tickers))
    return tickers


# ---------------------------------------------------------------------------
# A2. API key parameterisation
# ---------------------------------------------------------------------------
def load_api_key(env_path: Path) -> str:
    """Load STOOQ_API_KEY from config/.env. Hard-fail on missing/blank."""
    if not env_path.exists():
        raise FileNotFoundError(
            f".env file not found at {env_path}. "
            f"Create it with: STOOQ_API_KEY=your_key_here"
        )

    load_dotenv(dotenv_path=env_path, override=True)
    key = os.getenv("STOOQ_API_KEY", "").strip()

    if not key:
        raise ValueError(
            "STOOQ_API_KEY is missing or empty in config/.env. "
            "Get one by visiting https://stooq.com/q/d/?s=AAPL.US&get_apikey "
            "in a browser and copying the apikey value from the download URL."
        )

    log.info("STOOQ_API_KEY loaded (length=%d).", len(key))
    return key


# ---------------------------------------------------------------------------
# A3. Date range parameterisation
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Stooq EOD CSVs locally and upload to UC Volume."
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date YYYY-MM-DD (default: today - 90 days).",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date YYYY-MM-DD (default: today).",
    )
    return parser.parse_args()


def resolve_date_range(start: str | None, end: str | None) -> tuple[str, str]:
    """Resolve start/end to YYYYMMDD strings expected by Stooq."""
    today = datetime.now().date()
    end_date = datetime.strptime(end, "%Y-%m-%d").date() if end else today
    start_date = (
        datetime.strptime(start, "%Y-%m-%d").date()
        if start
        else end_date - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    )

    if start_date > end_date:
        raise ValueError(
            f"--start ({start_date}) must be on or before --end ({end_date})."
        )

    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    log.info("Date range: %s -> %s", start_str, end_str)
    return start_str, end_str


# ---------------------------------------------------------------------------
# A4. Download logic
# ---------------------------------------------------------------------------
def download_ticker(
    ticker: str,
    start: str,
    end: str,
    api_key: str,
    out_dir: Path,
) -> Path | None:
    """Fetch one ticker's daily CSV. Return the local path on success, None on skip."""
    url = STOOQ_URL_TEMPLATE.format(ticker=ticker, start=start, end=end, key=api_key)
    headers = {"User-Agent": USER_AGENT}
    out_path = out_dir / f"{ticker}_{start}_{end}.csv"

    # Log the URL with the key redacted so it stays out of logs/screenshots
    redacted_url = url.replace(api_key, "***REDACTED***")
    log.info("[%s] GET %s", ticker, redacted_url)

    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.RequestException as e:
        log.warning("[%s] Request failed: %s — skipping.", ticker, e)
        return None

    if resp.status_code != 200:
        log.warning(
            "[%s] HTTP %d — skipping. Body preview: %s",
            ticker, resp.status_code, resp.text[:120].replace("\n", " "),
        )
        return None

    body = resp.text.strip()
    if not body:
        log.warning("[%s] Empty response body — skipping.", ticker)
        return None

    # Stooq sometimes returns plaintext error messages with HTTP 200
    # (e.g. "No data" or "Exceeded the daily hits limit"). Sniff for that.
    first_line = body.splitlines()[0].lower()
    if "date" not in first_line:
        log.warning(
            "[%s] Response doesn't look like a CSV header (got: %r) — skipping.",
            ticker, body[:120],
        )
        return None

    out_path.write_text(resp.text, encoding="utf-8")
    log.info("[%s] Saved %d bytes -> %s", ticker, len(resp.content), out_path)
    return out_path


# ---------------------------------------------------------------------------
# A5. Upload to Unity Catalog Volume
# ---------------------------------------------------------------------------
def upload_to_volume(
    workspace: WorkspaceClient,
    local_path: Path,
    ticker: str,
    start: str,
    end: str,
) -> bool:
    """Upload one CSV to the UC Volume. Return True on success."""
    remote_path = f"{UC_VOLUME_PATH}/{ticker}_{start}_{end}.csv"
    try:
        with local_path.open("rb") as fh:
            workspace.files.upload(
                file_path=remote_path,
                contents=fh,
                overwrite=True,
            )
    except Exception as e:  # SDK raises a variety of exception types
        log.error("[%s] Upload failed: %s", ticker, e)
        return False

    log.info("[%s] Uploaded -> %s", ticker, remote_path)
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    args = parse_args()

    try:
        tickers = load_tickers(TICKERS_CSV)
        api_key = load_api_key(ENV_FILE)
        start, end = resolve_date_range(args.start, args.end)
    except (FileNotFoundError, ValueError) as e:
        log.error("Startup check failed: %s", e)
        return 1

    LOCAL_TMP_DIR.mkdir(parents=True, exist_ok=True)

    # Initialise SDK client once. Auth comes from ~/.databrickscfg.
    try:
        w = WorkspaceClient()
    except Exception as e:
        log.error(
            "Could not initialise Databricks WorkspaceClient. "
            "Check ~/.databrickscfg. Error: %s", e,
        )
        return 1

    attempted = len(tickers)
    downloaded: list[str] = []
    upload_succeeded: list[str] = []
    upload_failed: list[str] = []
    download_skipped: list[str] = []

    for idx, ticker in enumerate(tickers):
        local_csv = download_ticker(ticker, start, end, api_key, LOCAL_TMP_DIR)
        if local_csv is None:
            download_skipped.append(ticker)
        else:
            downloaded.append(ticker)
            ok = upload_to_volume(w, local_csv, ticker, start, end)
            (upload_succeeded if ok else upload_failed).append(ticker)

        # Polite pacing: only sleep between requests, not after the last one
        if idx < len(tickers) - 1:
            time.sleep(SLEEP_BETWEEN_REQUESTS_SECONDS)

    # Summary
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("  Attempted        : %d", attempted)
    log.info("  Downloaded       : %d  %s", len(downloaded), downloaded)
    log.info("  Download skipped : %d  %s", len(download_skipped), download_skipped)
    log.info("  Upload succeeded : %d  %s", len(upload_succeeded), upload_succeeded)
    log.info("  Upload failed    : %d  %s", len(upload_failed), upload_failed)
    log.info("=" * 60)

    return 0 if not upload_failed and not download_skipped else 2


if __name__ == "__main__":
    sys.exit(main())
