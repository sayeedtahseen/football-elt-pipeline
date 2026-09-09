"""API-Football HTTP client.

Phase 2: token-bucket limiter, tenacity retry, and the type-tolerant
``_validate`` that treats a non-empty ``errors`` (list or dict) as failure
even on HTTP 200. Exceptions come from ``elt.errors``.
"""


from typing import Iterator
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


_QUOTA_MARKERS = {
    "limit",
    "quota",
    "allowance",
    "exceeded",
    "too many requests"
}

_MAX_PAGES = 100

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
        """The critical check. API-Football returns HTTP 200 even on failure;
            ``errors`` is an empty list on success and a populated dict (or list) on
            failure. Reject a non-empty ``errors`` of either shape before the caller
            touches ``response``.
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




