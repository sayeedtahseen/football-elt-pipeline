"""CLI entrypoint for the ELT pipeline.

    python -m elt.run --mode {daily,backfill} [--endpoints ...] [--season ...] [--league ...]

Only argument parsing is wired up so far; the mode dispatch is Phase 2.
"""

import argparse
import sys

ENDPOINTS = ("leagues", "teams", "fixtures", "standings")


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


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    raise NotImplementedError(f"mode={args.mode!r} dispatch is implemented in Phase 2")


if __name__ == "__main__":
    sys.exit(main())
