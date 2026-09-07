"""Shared exception types for the ELT pipeline.

Kept in a leaf module (imports nothing else from ``elt``) so every other
module can raise these without risking a circular import.
"""


class ELTError(Exception):
    """Base class for every error this pipeline raises deliberately."""


class ApiFootballError(ELTError):
    """The API-Football envelope reported a failure.

    Raised whenever ``errors`` in the response body is non-empty, under
    either its list or dict shape. Note the HTTP status is often 200 even
    in this case, so this is the real success/failure signal.
    """


class QuotaExhaustedError(ApiFootballError):
    """The failure message indicates the request allowance is used up.

    A distinct type so the run can abort immediately instead of issuing
    more calls and risking the key being firewall-blocked.
    """
