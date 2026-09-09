"""One function per API-Football endpoint.

Each returns a list of raw rows ready for ``BigQueryRawLoader.append_rows`` --
one row per element of the API's ``response`` array, with the element kept whole
as a JSON string in ``payload``. No BigQuery writes and no run tracking here;
``elt.run`` orchestrates those.
"""

import hashlib
import json
from collections.abc import Iterable

from elt.api_football import ApiFootballClient

# endpoint name -> raw table it lands in. ``elt.run`` uses this map.
ENDPOINT_TABLES = {
    "leagues": "raw_leagues",
    "teams": "raw_teams",
    "fixtures": "raw_fixtures",
    "standings": "raw_standings",
}


def _rows_from(
    response: Iterable[dict],
    *,
    run_id: str,
    ingested_at: str,
    endpoint: str,
    params: dict,
) -> list[dict]:
    """Wrap each ``response`` element in the fixed 6-column raw schema.

    ``payload`` is the element re-serialized deterministically (sorted keys,
    no whitespace) so the same element yields the same bytes -- and therefore
    the same ``record_hash`` -- on every run.
    """
    params_json = json.dumps(params, sort_keys=True)
    rows: list[dict] = []
    for element in response:
        payload = json.dumps(element, sort_keys=True, separators=(",", ":"))
        rows.append(
            {
                "run_id": run_id,
                "ingested_at": ingested_at,
                "source_endpoint": endpoint,
                "request_params": params_json,
                "payload": payload,
                "record_hash": hashlib.sha256(payload.encode()).hexdigest(),
            }
        )
    return rows


def extract_leagues(
    client: ApiFootballClient, *, run_id: str, ingested_at: str, season: int
) -> list[dict]:
    params = {"season": season}
    return _rows_from(
        client.paginate("leagues", params),
        run_id=run_id,
        ingested_at=ingested_at,
        endpoint="leagues",
        params=params,
    )


def extract_teams(
    client: ApiFootballClient,
    *,
    run_id: str,
    ingested_at: str,
    league: int,
    season: int,
) -> list[dict]:
    params = {"league": league, "season": season}
    return _rows_from(
        client.paginate("teams", params),
        run_id=run_id,
        ingested_at=ingested_at,
        endpoint="teams",
        params=params,
    )


def extract_fixtures(
    client: ApiFootballClient,
    *,
    run_id: str,
    ingested_at: str,
    league: int,
    season: int,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    params: dict = {"league": league, "season": season}
    if date_from:
        params["from"] = date_from
    if date_to:
        params["to"] = date_to
    return _rows_from(
        client.paginate("fixtures", params),
        run_id=run_id,
        ingested_at=ingested_at,
        endpoint="fixtures",
        params=params,
    )


def extract_standings(
    client: ApiFootballClient,
    *,
    run_id: str,
    ingested_at: str,
    league: int,
    season: int,
) -> list[dict]:
    params = {"league": league, "season": season}
    return _rows_from(
        client.paginate("standings", params),
        run_id=run_id,
        ingested_at=ingested_at,
        endpoint="standings",
        params=params,
    )
