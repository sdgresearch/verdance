"""Overpass queries are retried while the server is overloaded, and only then."""

import pytest
from osmnx._errors import ResponseStatusCodeError

from verdance import osm


def _flaky(responses):
    calls = []

    def query(*args):
        calls.append(args)
        status = responses[len(calls) - 1]
        if status:
            raise ResponseStatusCodeError(f"'overpass-api.de' responded: {status} Bad Gateway")
        return "result"

    return query, calls


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch):
    monkeypatch.setattr(osm, "OVERPASS_FIRST_WAIT", 0)


def test_overloaded_server_is_retried():
    query, calls = _flaky([502, 429, None])
    assert osm._overpass(query, "polygon") == "result"
    assert calls == [("polygon",)] * 3


def test_client_errors_are_not_retried():
    query, calls = _flaky([400, None])
    with pytest.raises(ResponseStatusCodeError):
        osm._overpass(query)
    assert len(calls) == 1


def test_gives_up_after_the_last_retry(monkeypatch):
    monkeypatch.setattr(osm, "OVERPASS_RETRIES", 2)
    query, calls = _flaky([503, 503, 503, None])
    with pytest.raises(ResponseStatusCodeError):
        osm._overpass(query)
    assert len(calls) == 3
