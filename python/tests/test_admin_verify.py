import gzip
import json
from unittest.mock import Mock

import click
import pytest
import requests

from iomaps.commands import admin_verify as verify


@pytest.fixture
def plan():
    return {
        "route_repository": "ramSeraph/indianopenmaps",
        "route_entry": {"/plots/test/": {"name": "Test", "promoteid": "id"}},
        "uploads": [{"name": "Test.pmtiles", "path": "/local/Test.pmtiles"}],
    }


@pytest.fixture
def sample():
    return {"z": 4, "x": 11, "y": 6, "data": b"sample tile", "layers": [{"id": "Test"}]}


@pytest.fixture
def fake_clock(monkeypatch):
    clock = [0]
    monkeypatch.setattr(verify.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(verify.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    return clock


def response(data=None, content=None):
    result = Mock()
    result.json.return_value = data
    result.content = content
    return result


def fake_requests(monkeypatch, plan, sample, *, pending=False, wrong_tile=False, failure=False):
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        if "check-runs" in url:
            return response(
                {
                    "check_runs": [
                        {
                            "id": 1,
                            "name": "Workers Builds: indianopenmaps",
                            "status": "in_progress" if pending else "completed",
                            "conclusion": "failure" if failure else "success",
                            "details_url": "https://example.com/build",
                        }
                    ]
                }
            )
        if url.endswith("/api/routes"):
            return response(plan["route_entry"])
        if url.endswith("/tiles.json"):
            return response(
                {
                    "vector_layers": sample["layers"],
                    "minzoom": 0,
                    "maxzoom": 16,
                    "tiles": ["https://indianopenmaps.com/plots/test/{z}/{x}/{y}.pbf"],
                }
            )
        return response(content=gzip.compress(b"wrong" if wrong_tile else sample["data"]))

    monkeypatch.setattr(verify.requests, "get", get)
    return calls


def test_live_verification_checks_build_route_metadata_and_tile(plan, sample, monkeypatch, fake_clock):
    calls = fake_requests(monkeypatch, plan, sample)
    result = verify.verify_live(plan, "commit-sha", sample, 30)
    assert result["status"] == "verified"
    assert result["tile_matches_local"] is True
    assert len(calls) == 4
    assert "commit-sha/check-runs" in calls[0]
    assert calls[-1].endswith("/4/11/6.pbf")


@pytest.mark.parametrize("problem", ["pending", "wrong_tile"])
def test_live_verification_timeout_does_not_claim_success(plan, sample, monkeypatch, fake_clock, problem):
    fake_requests(monkeypatch, plan, sample, **{problem: True})
    with pytest.raises(click.ClickException, match="Published, but live verification timed out"):
        verify.verify_live(plan, "commit-sha", sample, 15)
    assert fake_clock[0] == 15


def test_failed_build_is_reported_immediately(plan, sample, monkeypatch, fake_clock):
    fake_requests(monkeypatch, plan, sample, failure=True)
    with pytest.raises(click.ClickException, match="Cloudflare build failure"):
        verify.verify_live(plan, "commit-sha", sample, 30)
    assert fake_clock[0] == 0


def test_transient_network_failure_is_retried(plan, sample, monkeypatch, fake_clock):
    fake_requests(monkeypatch, plan, sample)
    get = verify.requests.get
    failures = [True]

    def transient(*args, **kwargs):
        if failures:
            failures.pop()
            raise requests.ConnectionError("temporary failure")
        return get(*args, **kwargs)

    monkeypatch.setattr(verify.requests, "get", transient)
    assert verify.verify_live(plan, "commit-sha", sample, 30)["status"] == "verified"
    assert fake_clock[0] == 10


@pytest.mark.parametrize("field", ["id", "OGC_FID"])
def test_local_sample_and_promoteid_are_checked_before_uploads(plan, monkeypatch, field):
    plan["route_entry"]["/plots/test/"]["promoteid"] = field

    def pmtiles(*args):
        if args[-1] == "--header-json":
            return json.dumps({"center": [77, 28, 4], "minzoom": 0, "maxzoom": 16}).encode()
        if args[-1] == "--metadata":
            return json.dumps({"vector_layers": [{"id": "Test", "fields": {"id": "String"}}]}).encode()
        return gzip.compress(b"sample tile")

    monkeypatch.setattr(verify, "pmtiles", pmtiles)
    if field == "OGC_FID":
        with pytest.raises(click.ClickException, match="absent from the tiles"):
            verify.choose_sample_tile(plan)
    else:
        assert verify.choose_sample_tile(plan)["data"] == b"sample tile"


@pytest.mark.parametrize("wrong_tile", [False, True])
def test_live_verification_uses_remote_sample_checksum(plan, sample, monkeypatch, fake_clock, wrong_tile):
    fake_requests(monkeypatch, plan, sample, wrong_tile=wrong_tile)
    handoff = json.loads(json.dumps(verify.sample_manifest(sample)))
    assert "data" not in handoff
    if wrong_tile:
        with pytest.raises(click.ClickException, match="live sample tile differs"):
            verify.verify_live(plan, "commit-sha", handoff, 15)
    else:
        assert verify.verify_live(plan, "commit-sha", handoff, 30)["tile_matches_local"]
