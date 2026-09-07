"""API-Football HTTP client.

Phase 2: token-bucket limiter, tenacity retry, and the type-tolerant
``_validate`` that treats a non-empty ``errors`` (list or dict) as failure
even on HTTP 200. Exceptions come from ``elt.errors``.
"""


class ApiFootballClient:
    """Single client for one API-Football subscription (RapidAPI or API-Sports)."""
