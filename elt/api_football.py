"""API-Football HTTP client.

Phase 2: token-bucket limiter, tenacity retry, and the type-tolerant
``_validate`` that treats a non-empty ``errors`` (list or dict) as failure.
Most API-Football failures arrive as HTTP 200 with a populated ``errors``
(wrong-but-well-formed key, exhausted quota, missing/unknown params); only a
missing or malformed key header is rejected at the edge with a real 403, which
``get`` catches via the ``status >= 400`` check. Exceptions come from
``elt.errors``.
"""


from typing import Iterator
import logging
import time

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter
)

from elt.config import Settings
from elt.errors import ApiFootballError, QuotaExhaustedError, RetryableHTTPError


log = logging.getLogger(__name__)

_QUOTA_MARKERS = {
    "limit",
    "quota",
    "allowance",
    "exceeded",
    "too many requests"
}

_MAX_PAGES = 100

# Warn once the daily allowance drops below this fraction of the plan's limit.
_QUOTA_WARN_FRACTION = 0.1

class ApiFootballClient:
    """Single client for the API-Football (API-Sports) subscription.

    Auth is ``x-apisports-key`` against ``https://<API_FOOTBALL_HOST>``
    (``v3.football.api-sports.io``); the host already carries the ``/v3``.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.session = requests.Session()
        self.base_url = f"https://{settings.api_football_host}"
        self._min_interval = 60.0 / settings.api_rate_limit_rpm
        self._last_request = 0.0

    def _headers(self) -> dict[str, str]:
        return {"x-apisports-key": self.settings.api_football_key}

    def _wait_for_slot(self) -> None:
        wait = self._min_interval - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()


    @retry(
        retry=retry_if_exception_type((RetryableHTTPError, requests.ConnectionError, requests.Timeout)),
        wait=wait_exponential_jitter(initial=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True
    )
    def get(self, endpoint: str, params: dict) -> dict:
        self._wait_for_slot()

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        resp = self.session.get(url, headers=self._headers(), params=params, timeout=30)

        # Before the status branching: an error response carries these headers
        # too, and a 429 is exactly when the numbers are worth having.
        self._log_quota(resp)

        status = resp.status_code
        if status == 429 or status == 499 or status >= 500:
            raise RetryableHTTPError(status, f"{endpoint} -> HTTP {status}")
        if status >= 400:
            raise ApiFootballError(f"{endpoint} -> HTTP {status} (not retried): {resp.text[:200]}")

        try:
            body = resp.json()
        except ValueError as exception:
            raise ApiFootballError(f"{endpoint} -> non-JSON response: {resp.text[:200]}") from exception

        self._validate(body)
        return body


    def _log_quota(self, resp: requests.Response) -> None:
        """Log the rate-limit headers API-Sports returns on every response.

        ``x-ratelimit-requests-remaining`` / ``-limit`` are the daily allowance
        (7500/day on Pro); ``x-ratelimit-remaining`` / ``-limit`` are the
        per-minute one (300/min). Cheap observability: it turns "are we about to
        run out of quota" from a guess into a number in the logs. Best-effort --
        a missing or non-numeric header must never take down a request that
        otherwise succeeded.
        """
        daily_remaining = resp.headers.get("x-ratelimit-requests-remaining")
        daily_limit = resp.headers.get("x-ratelimit-requests-limit")
        minute_remaining = resp.headers.get("x-ratelimit-remaining")
        minute_limit = resp.headers.get("x-ratelimit-limit")

        log.debug(
            "quota: daily %s/%s, minute %s/%s",
            daily_remaining,
            daily_limit,
            minute_remaining,
            minute_limit,
        )

        try:
            if int(daily_remaining) <= int(daily_limit) * _QUOTA_WARN_FRACTION:
                log.warning(
                    "daily quota low: %s of %s requests remaining",
                    daily_remaining,
                    daily_limit,
                )
        except (TypeError, ValueError):
            pass  # header absent or not a number -- nothing to warn about


    def paginate(self, endpoint: str, params: dict) -> Iterator[dict]:
        page = 1
        while True:
            # Non-paginating endpoints (leagues, teams, standings) 400 on an
            # unknown ``page`` param; page 1 is identical to omitting it.
            call_params = params if page == 1 else {**params, "page": page}
            body = self.get(endpoint, call_params)
            yield from body.get("response") or []

            paging = body.get("paging") or {}
            try:
                total = int(paging.get("total") or 1)
            except (TypeError, ValueError):
                total = 1


            if total > _MAX_PAGES:
                raise ApiFootballError(
                    f"{endpoint} reports {total} pages (> {_MAX_PAGES}): refusing to iterate"
                )
            if page >= total:
                return
            page += 1


    def _validate(self, body) -> None:
        """The critical check. API-Football returns HTTP 200 on most failures
            (wrong-but-well-formed key, exhausted quota, missing/unknown params);
            ``errors`` is an empty list on success and a populated dict on failure
            (a list shape is handled too, defensively). Reject a non-empty
            ``errors`` of either shape before the caller touches ``response``.
            A missing/malformed key header is the exception: a real 403 that
            ``get`` has already raised on before reaching here.
        """
        
        errors = body.get("errors")
        if not errors:
            return

        if isinstance(errors, dict):
            message = "; ".join(f"{key}: {value}" for key, value in errors.items())
        elif isinstance(errors, list):
            message = "; ".join(str(item) for item in errors)
        else:
            message = str(errors)

        if self._looks_like_quota(message):
            raise QuotaExhaustedError(message)
        raise ApiFootballError(message)

    def _looks_like_quota(self, message: str) -> bool:
        msg = message.lower()
        return any(marker in msg for marker in _QUOTA_MARKERS)




