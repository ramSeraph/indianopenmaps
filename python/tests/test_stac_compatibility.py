"""Opt-in checks of a running API using an independent STAC client.

STAC_TEST_URL=http://127.0.0.1:8791/stac pytest tests/test_stac_compatibility.py
Also supports the deployed URL. Does not exercise the direct-release iomaps client.
"""

import os

import pytest
from pystac_client import Client

URL = os.environ.get("STAC_TEST_URL")
pytestmark = pytest.mark.skipif(not URL, reason="Set STAC_TEST_URL to test a running API")


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_pystac_discovery_complete_pagination_and_spatial_temporal_filters(method):
    client = Client.open(URL)
    collections = list(client.get_collections())
    assert collections
    for collection in collections:
        items = list(client.search(collections=[collection.id], method=method, limit=317).items())
        assert len(items) == collection.extra_fields["item_count"]
        assert len({item.id for item in items}) == len(items)
        late = items[-1]
        assert client.get_collection(collection.id).get_item(late.id).id == late.id
        matches = list(
            client.search(
                collections=[collection.id], method=method, bbox=late.bbox, datetime=late.datetime.isoformat(), limit=7
            ).items()
        )
        assert late.id in {item.id for item in matches}
        assert all(item.collection_id == collection.id for item in matches)
        assert not list(
            client.search(
                collections=[collection.id], method=method, datetime="1900-01-01T00:00:00Z/1900-01-02T00:00:00Z"
            ).items()
        )
        late.validate()
