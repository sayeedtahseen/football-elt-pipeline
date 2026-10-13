"""Tests for ``elt.api_football.ApiFootballClient``.

API-Football returns HTTP 200 on most failures (wrong-but-well-formed key,
exhausted quota, bad params) with a populated ``errors`` field -- this suite
exists to pin down that the envelope, not the status code, is what the client
trusts. Every HTTP interaction is mocked with ``responses`` against captured
envelopes in ``tests/fixtures/``; nothing here makes a real network call.
"""

import json
from pathlib import Path

import pytest
import responses

from elt.errors import ApiFootballError, QuotaExhaustedError

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text())


def url_for(client, endpoint: str) -> str:
    return f"{client.base_url}/{endpoint}"


class TestValidateAndGet:
    @responses.activate
    def test_success_envelope_parses_and_yields_right_count(self, client):
        body = load_fixture("leagues_success.json")
        responses.add(responses.GET, url_for(client, "leagues"), json=body, status=200)

        result = client.get("leagues", {"season": 2025})

        assert result == body
        assert len(result["response"]) == 2

    @responses.activate
    def test_errors_dict_with_http_200_raises_api_football_error(self, client):
        """The headline bug: a 200 with a populated ``errors`` dict must raise,
        not be treated as success."""
        body = load_fixture("error_invalid_key.json")
        responses.add(responses.GET, url_for(client, "leagues"), json=body, status=200)

        with pytest.raises(ApiFootballError):
            client.get("leagues", {"season": 2025})

    @responses.activate
    def test_errors_empty_list_does_not_raise(self, client):
        body = load_fixture("leagues_success.json")
        assert body["errors"] == []
        responses.add(responses.GET, url_for(client, "leagues"), json=body, status=200)

        client.get("leagues", {"season": 2025})  # should not raise

    @responses.activate
    def test_quota_message_raises_quota_exhausted_error(self, client):
        body = load_fixture("error_quota.json")
        responses.add(responses.GET, url_for(client, "leagues"), json=body, status=200)

        with pytest.raises(QuotaExhaustedError):
            client.get("leagues", {"season": 2025})

    @responses.activate
    def test_non_quota_error_raises_base_error_not_quota_exhausted(self, client):
        """Guards against the quota-marker check being too eager: a bad-key
        message contains no quota marker words and must stay a plain
        ApiFootballError."""
        body = load_fixture("error_invalid_key.json")
        responses.add(responses.GET, url_for(client, "leagues"), json=body, status=200)

        with pytest.raises(ApiFootballError) as exc_info:
            client.get("leagues", {"season": 2025})
        assert not isinstance(exc_info.value, QuotaExhaustedError)


class TestPagination:
    @responses.activate
    def test_multi_page_issues_one_request_per_page_and_yields_all_records(self, client):
        for page in (1, 2, 3):
            responses.add(
                responses.GET,
                url_for(client, "fixtures"),
                json=load_fixture(f"fixtures_page{page}.json"),
                status=200,
            )

        records = list(client.paginate("fixtures", {"league": 39, "season": 2025}))

        assert len(responses.calls) == 3
        assert [r["fixture"]["id"] for r in records] == [1001, 1002, 1003]

    @responses.activate
    def test_absurd_total_pages_raises_without_further_requests(self, client):
        body = load_fixture("fixtures_page1.json")
        body["paging"]["total"] = 9999
        responses.add(responses.GET, url_for(client, "fixtures"), json=body, status=200)

        with pytest.raises(ApiFootballError):
            list(client.paginate("fixtures", {"league": 39, "season": 2025}))

        assert len(responses.calls) == 1

    @responses.activate
    def test_missing_paging_block_is_treated_as_single_page(self, client):
        body = load_fixture("fixtures_page1.json")
        del body["paging"]
        responses.add(responses.GET, url_for(client, "fixtures"), json=body, status=200)

        records = list(client.paginate("fixtures", {"league": 39, "season": 2025}))

        assert len(responses.calls) == 1
        assert [r["fixture"]["id"] for r in records] == [1001]


class TestRetryBehavior:
    @responses.activate
    def test_429_retries_then_succeeds(self, client):
        success_body = load_fixture("leagues_success.json")
        responses.add(responses.GET, url_for(client, "leagues"), status=429)
        responses.add(responses.GET, url_for(client, "leagues"), json=success_body, status=200)

        result = client.get("leagues", {"season": 2025})

        assert result == success_body
        assert len(responses.calls) == 2

    @responses.activate
    def test_401_does_not_retry(self, client):
        responses.add(
            responses.GET,
            url_for(client, "leagues"),
            status=401,
            body="unauthorized",
        )

        with pytest.raises(ApiFootballError):
            client.get("leagues", {"season": 2025})

        assert len(responses.calls) == 1
