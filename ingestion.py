"""Extract match data from the football-data.org API and land it as raw JSON in S3.
 
Every run reconciles the landing zone against the seasons we want:
  - the current season is snapshotted on every run
  - each past season (from EARLIEST_SEASON) is fetched until a final,
    complete snapshot exists, then marked with a _COMPLETE object and skipped
 
Usage:
    python ingestion.py                          # normal run (daily)
    python ingestion.py --seasons 2024           # only process specific seasons
    python ingestion.py --seasons 2024 --force   # re-fetch even if marked complete
    python ingestion.py --competitions PL ELC    # limit to specific competitions
 
Environment variables (from .env locally):
    FOOTBALL_DATA_API_KEY   football-data.org API key
    RAW_BUCKET              name of the S3 bucket for raw files
AWS credentials are picked up automatically by boto3 (e.g. from ~/.aws/credentials).
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
 
import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

BASE_URL = "https://api.football-data.org/v4"
RAW_PREFIX = "matches"  # top-level folder for this dataset inside the raw bucket
REQUEST_TIMEOUT = 30  # seconds to wait for a response before giving up
MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

COMPETITIONS = {
    "PL": "Premier League",
}

# Oldest season (start year) to keep. Set this to the earliest season the
# free tier actually returns for you.
EARLIEST_SEASON = 2023

# Match statuses that will never change again
TERMINAL_STATUSES = {"FINISHED", "AWARDED", "CANCELLED"}

# If a season ended this long ago, accept it as complete even if a few matches
# never reached a terminal status (e.g. a postponed game that was never played)
COMPLETENESS_GRACE_DAYS = 30

COMPLETE_MARKER = "_COMPLETE"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# --- Configuration -----------------------------------------------------------
 
 
def get_required_env(name: str) -> str:
    """Read a required environment variable, failing clearly if it's missing."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is not set. Add it to your .env file.")
    return value


# --- API access --------------------------------------------------------------


def create_session(api_key: str) -> requests.Session:
    """Create a session that sends the auth header on every request."""
    session = requests.Session()
    session.headers.update({"X-Auth-Token": api_key})
    return session


def get_with_retries(session: requests.Session, url: str, params: dict) -> dict:
    """GET a URL, retrying on network errors, rate limits and server errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        logger.info("GET %s params=%s (attempt %d/%d)", url, params, attempt, MAX_RETRIES)

        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except (requests.ConnectionError, requests.Timeout) as exc:
            wait = 2**attempt
            logger.warning("Network error: %s. Retrying in %ds", exc, wait)
            time.sleep(wait)
            continue

        if response.status_code in RETRYABLE_STATUS_CODES:
            if response.status_code == 429:
                # Rate limited: the API says how many seconds until the counter resets
                wait = int(response.headers.get("X-RequestCounter-Reset", 60))
            else:
                wait = 2**attempt
            logger.warning(
                "HTTP %d from API. Retrying in %ds", response.status_code, wait
            )
            time.sleep(wait)
            continue

        if not response.ok:
            # Other 4xx errors (bad parameters, no access to a season or competition):
            # retrying won't help, so log the API's message and fail
            logger.error("HTTP %d from API: %s", response.status_code, response.text)
            response.raise_for_status()

        return response.json()

    raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} attempts")


def get_current_season(session: requests.Session, competition: str) -> int:
    """Return the start year of the competition's current season, e.g. 2026."""
    data = get_with_retries(session, f"{BASE_URL}/competitions/{competition}", params={})
    start_date = data["currentSeason"]["startDate"]  # e.g. "2026-08-15"
    return int(start_date[:4])


def get_matches(
        session: requests.Session,
        competition: str,
        *,
        season: int | None = None,
        date_from: str | None = None,  # "YYYY-MM-DD"
        date_to: str | None = None,  # "YYYY-MM-DD"
        status: str | None = None,  # e.g. "FINISHED", "SCHEDULED"
        matchday: int | None = None,
        stage: str | None = None,
        group: str | None = None,
    ) -> dict:
    """Fetch matches for one competition, applying any filters given."""
    params = {
        "season": season,
        "dateFrom": date_from,
        "dateTo": date_to,
        "status": status,
        "matchday": matchday,
        "stage": stage,
        "group": group,
    }
    # Only send the filters that were actually set
    params = {key: value for key, value in params.items() if value is not None}

    url = f"{BASE_URL}/competitions/{competition}/matches"
    return get_with_retries(session, url, params)


# --- Completeness -------------------------------------------------------------


def is_season_complete(data: dict, today: date) -> bool:
    """A season is complete when every match has reached a terminal status,
    or when the season ended long enough ago that nothing more will change."""
    matches = data.get("matches", [])
    if not matches:
        return False

    non_terminal = [m for m in matches if m.get("status") not in TERMINAL_STATUSES]
    if not non_terminal:
        return True

    end_date = date.fromisoformat(matches[0]["season"]["endDate"])
    if today > end_date + timedelta(days=COMPLETENESS_GRACE_DAYS):
        logger.warning(
            "Season ended %s but %d matches are still non-terminal "
            "(statuses: %s). Treating as complete after grace period.",
            end_date,
            len(non_terminal),
            sorted({m.get("status") for m in non_terminal}),
        )
        return True

    return False


# --- Landing raw files -------------------------------------------------------


def season_prefix(competition: str, season: int) -> str:
    return f"{RAW_PREFIX}/competition={competition}/season={season}"
 
 
def is_marked_complete(s3, bucket: str, competition: str, season: int) -> bool:
    """True if the season's _COMPLETE marker object exists."""
    key = f"{season_prefix(competition, season)}/{COMPLETE_MARKER}"
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise  # anything else (e.g. permissions) is a real error
 
 
def write_raw(
        s3, bucket: str, data: dict, competition: str, season: int, ingested_at: datetime
    ) -> str:
    """Upload the API response unchanged as a new, never-overwritten object."""
    key = (
        f"{season_prefix(competition, season)}"
        f"/ingest_date={ingested_at:%Y-%m-%d}"
        f"/matches_{ingested_at:%Y%m%dT%H%M%SZ}.json"
    )
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Wrote s3://%s/%s", bucket, key)
    return key
 
 
def mark_complete(
        s3, bucket: str, competition: str, season: int, snapshot_key: str
    ) -> None:
    """Write a marker recording which snapshot is the final one for the season."""
    key = f"{season_prefix(competition, season)}/{COMPLETE_MARKER}"
    body = {
        "final_snapshot": snapshot_key,
        "marked_at": datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(body).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Marked %s %d complete", competition, season)


# --- A single run ------------------------------------------------------------


def ingest_season(
        session: requests.Session,
        s3,
        bucket: str,
        competition: str,
        season: int,
        current_season: int,
        force: bool,
    ) -> None:
    is_past_season = season < current_season
 
    if is_past_season and not force and is_marked_complete(s3, bucket, competition, season):
        logger.info("Skipping %s %d: marked complete", competition, season)
        return
 
    data = get_matches(session, competition, season=season)
    match_count = len(data.get("matches", []))
    if match_count == 0:
        logger.warning("%s %d returned no matches", competition, season)
 
    ingested_at = datetime.now(timezone.utc)
    key = write_raw(s3, bucket, data, competition, season, ingested_at)
    logger.info("%s %d: landed %d matches", competition, season, match_count)
 
    # Only past seasons get marked: the current season is always re-snapshotted
    if is_past_season and is_season_complete(data, ingested_at.date()):
        mark_complete(s3, bucket, competition, season, key)
 
 
def ingest_competition(
        session: requests.Session,
        s3,
        bucket: str,
        competition: str,
        seasons: list[int] | None,
        force: bool,
    ) -> None:
    current_season = get_current_season(session, competition)
    seasons_to_process = seasons or list(range(EARLIEST_SEASON, current_season + 1))
 
    for season in seasons_to_process:
        ingest_season(session, s3, bucket, competition, season, current_season, force)
 
 
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Land football-data.org match data as raw JSON in S3."
    )
    parser.add_argument(
        "--seasons",
        type=int,
        nargs="+",
        help="Only process these season start years, e.g. 2024 2025. "
        f"Default: every season from {EARLIEST_SEASON} to the current one.",
    )
    parser.add_argument(
        "--competitions",
        nargs="+",
        default=list(COMPETITIONS),
        help="Competition codes to fetch (default: all configured).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch past seasons even if marked complete.",
    )
    return parser.parse_args(argv)
 
 
def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv()
 
    session = create_session(get_required_env("FOOTBALL_DATA_API_KEY"))
    bucket = get_required_env("RAW_BUCKET")
    s3 = boto3.client("s3")
 
    failures = []
    for code in args.competitions:
        name = COMPETITIONS.get(code, code)
        try:
            ingest_competition(session, s3, bucket, code, args.seasons, args.force)
        except (
            requests.RequestException,
            BotoCoreError,
            ClientError,
            RuntimeError,
            KeyError,
            ValueError,
        ):
            logger.exception("Failed to ingest %s (%s)", name, code)
            failures.append(code)
 
    if failures:
        logger.error("Ingestion failed for: %s", ", ".join(failures))
        return 1
 
    logger.info("Ingestion complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
