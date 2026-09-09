"""CLI entrypoint for the ELT pipeline.

    python -m elt.run --mode {daily,backfill} [--endpoints ...] [--season ...] [--league ...]

Only argument parsing is wired up so far; the mode dispatch is Phase 2.
"""

import argparse
from datetime import date, datetime, timedelta, timezone
import logging
import sys
import uuid

from elt import extractors
from elt.api_football import ApiFootballClient
from elt.bigquery import BigQueryRawLoader
from elt.config import load_settings
from elt.errors import ELTError, QuotaExhaustedError
from elt.state import IngestionRunTracker

log = logging.getLogger(__name__)

ENDPOINTS = ("leagues", "teams", "fixtures", "standings")
EXTRACTORS = {
    "leagues": extractors.extract_leagues,
    "teams": extractors.extract_teams,
    "fixtures": extractors.extract_fixtures,
    "standings": extractors.extract_standings,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="elt.run",
        description="Extract API-Football data and load raw JSON into BigQuery.",
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("daily", "backfill"),
        help="daily: leagues/teams/standings in full + a fixtures window. "
        "backfill: every endpoint for every configured league x season.",
    )
    parser.add_argument(
        "--endpoints",
        nargs="+",
        choices=ENDPOINTS,
        metavar="ENDPOINT",
        help=f"subset of endpoints to run (default: all of {', '.join(ENDPOINTS)}).",
    )
    parser.add_argument(
        "--season",
        type=int,
        action="append",
        dest="seasons",
        metavar="YEAR",
        help="override SEASONS from env; repeatable (e.g. --season 2024 --season 2025).",
    )
    parser.add_argument(
        "--league",
        type=int,
        action="append",
        dest="leagues",
        metavar="ID",
        help="override LEAGUES from env; repeatable (e.g. --league 39 --league 140).",
    )
    return parser

def _plan_tasks(args, settings) -> list[tuple[str, dict]]:
    """Expand a mode into the concrete list of extractor calls.

    A task is ``(endpoint, kwargs)`` where kwargs are the extractor's
    endpoint-specific keyword args (league/season/date_from/date_to). run_id,
    ingested_at and the client are supplied at execution time.
    """
    seasons = args.seasons or list(settings.seasons)
    leagues = args.leagues or list(settings.leagues)
    wanted = set(args.endpoints) if args.endpoints else set(ENDPOINTS)

    if args.mode == "daily":
        today = date.today()
        fixture_window = {
            "date_from": (today - timedelta(days=settings.daily_lookback_days)).isoformat(),
            "date_to": (today + timedelta(days=settings.daily_lookahead_days)).isoformat(),
        }
    else:  # backfill: whole season, no window
        fixture_window = {}

    tasks: list[tuple[str, dict]] = []
    for season in seasons:
        if "leagues" in wanted:
            tasks.append(("leagues", {"season": season}))
        for league in leagues:
            if "teams" in wanted:
                tasks.append(("teams", {"league": league, "season": season}))
            if "standings" in wanted:
                tasks.append(("standings", {"league": league, "season": season}))
            if "fixtures" in wanted:
                tasks.append(
                    ("fixtures", {"league": league, "season": season, **fixture_window})
                )

    log.info("planned %d tasks (mode=%s)", len(tasks), args.mode)
    return tasks


def _execute(tasks, client, loader, tracker, run_id, ingested_at) -> list[str]:
    """Run each task: track start -> extract -> load -> track finish.

    Returns the list of endpoints that failed (empty = clean run). A
    ``QuotaExhaustedError`` aborts the whole run (it propagates); any other
    extractor failure is recorded and the remaining tasks still run.
    """
    failed: list[str] = []

    for endpoint, params in tasks:
        extractor = EXTRACTORS[endpoint]
        table = extractors.ENDPOINT_TABLES[endpoint]
        tracker.start_run(run_id, endpoint, params)

        try:
            rows = extractor(client, run_id=run_id, ingested_at=ingested_at, **params)
            n = loader.append_rows(table, rows)
            tracker.finish_run(run_id, endpoint, status="success", rows_loaded=n)
            log.info("%s %s: %d rows -> %s", endpoint, params, n, table)
        except QuotaExhaustedError as exc:
            tracker.finish_run(run_id, endpoint, status="error", error_message=str(exc))
            log.error("quota exhausted on %s; aborting run: %s", endpoint, exc)
            failed.append(endpoint)
            raise
        except ELTError as exc:
            # An expected pipeline error (bad envelope, HTTP 4xx, ...). The
            # message is the signal; a full traceback is just noise.
            tracker.finish_run(run_id, endpoint, status="error", error_message=str(exc))
            log.error("%s failed: %s", endpoint, exc)
            failed.append(endpoint)
        except Exception as exc:  # noqa: BLE001 -- one bad endpoint must not stop the rest
            # Genuinely unexpected -- keep the traceback for these.
            tracker.finish_run(run_id, endpoint, status="error", error_message=str(exc))
            log.exception("%s failed unexpectedly", endpoint)
            failed.append(endpoint)

    return failed


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        settings = load_settings()
    except ELTError as exc:
        logging.basicConfig(level="INFO")
        log.error("configuration error: %s", exc)
        return 1

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )

    client = ApiFootballClient(settings)
    loader = BigQueryRawLoader(settings)
    tracker = IngestionRunTracker(loader)

    run_id = uuid.uuid4().hex
    ingested_at = datetime.now(timezone.utc).isoformat()
    log.info("run %s starting (mode=%s)", run_id, args.mode)

    tasks = _plan_tasks(args, settings)
    try:
        failed = _execute(tasks, client, loader, tracker, run_id, ingested_at)
    except QuotaExhaustedError:
        return 1

    if failed:
        log.error(
            "run %s finished with %d failed endpoint(s): %s",
            run_id,
            len(failed),
            ", ".join(failed),
        )
        return 1

    log.info("run %s completed cleanly (%d tasks)", run_id, len(tasks))
    return 0



if __name__ == "__main__":
    sys.exit(main())
