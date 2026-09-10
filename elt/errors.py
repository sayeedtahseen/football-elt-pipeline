"""Shared exception types for the ELT pipeline.

Kept in a leaf module (imports nothing else from ``elt``) so every other
module can raise these without risking a circular import.
"""


class ELTError(Exception):
    """Base class for every error this pipeline raises deliberately."""


class ApiFootballError(ELTError):
    """The API-Football envelope reported a failure.

    Raised whenever ``errors`` in the response body is non-empty, under
    either its list or dict shape. The HTTP status is usually 200 in this
    case (wrong-but-well-formed key, quota, bad params), so the envelope --
    not the status code -- is the real success/failure signal. Also raised
    for a non-retryable HTTP >= 400 (e.g. the edge 403 for a missing or
    malformed key header).
    """


class QuotaExhaustedError(ApiFootballError):
    """The failure message indicates the request allowance is used up.

    A distinct type so the run can abort immediately instead of issuing
    more calls and risking the key being firewall-blocked.
    """

class RetryableHTTPError(ELTError):
    """A transient HTTP failure (429, 499, or 5xx) worth retrying with backoff.

    Carries the status code. Never raised for a 4xx auth error -- those are
    permanent and must not be retried.
    """

    def __init__(self, status_code: int, message: str = ""):
        self.status_code = status_code
        super().__init__(message or f"retryable HTTP {status_code}")