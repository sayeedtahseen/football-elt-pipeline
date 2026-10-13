"""Shared pytest fixtures for the ELT test suite.

No live network calls anywhere in this suite -- every HTTP interaction goes
through ``responses`` against captured envelopes in ``tests/fixtures/``.
"""

import pytest

from elt.api_football import ApiFootballClient
from elt.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        api_football_key="test-key",
        api_football_host="v3.football.api-sports.io",
        # Effectively no throttling -- _wait_for_slot must not slow the suite down.
        api_rate_limit_rpm=1_000_000,
        gcp_project="test-project",
        leagues=[39],
        seasons=[2025],
    )


@pytest.fixture
def client(settings: Settings) -> ApiFootballClient:
    return ApiFootballClient(settings)
