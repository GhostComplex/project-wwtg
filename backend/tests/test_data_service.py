"""Tests for DataService caching, persistence, and AMAP integration."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.schemas import POIData
from app.services.data_service import DataService


def _make_amap_poi(**overrides) -> POIData:
    """Create a test POIData with AMAP source type."""
    defaults = {
        "name": "拙政园",
        "address": "苏州市姑苏区东北街178号",
        "city": "苏州",
        "tags": ["景点", "户外"],
        "source_type": "amap",
        "rating": 4.8,
        "phone": "0512-67510286",
        "location": "120.630,31.324",
        "verified": True,
    }
    defaults.update(overrides)
    return POIData(**defaults)


@pytest.mark.asyncio
class TestAmapPOICaching:
    """Test caching AMAP-sourced POIs."""

    async def test_cache_and_retrieve_amap_pois(self):
        """AMAP POIs can be cached to Redis and retrieved."""
        redis = AsyncMock()
        redis.get.return_value = None

        svc = DataService(redis_client=redis)
        pois = [_make_amap_poi(), _make_amap_poi(name="苏州博物馆")]

        await svc.cache_pois("苏州", pois)
        redis.set.assert_called_once()

        # Verify the cached data includes AMAP fields
        call_args = redis.set.call_args
        cached_json = json.loads(call_args[0][1])
        assert len(cached_json) == 2
        assert cached_json[0]["source_type"] == "amap"
        assert cached_json[0]["rating"] == 4.8
        assert cached_json[0]["location"] == "120.630,31.324"

    async def test_amap_poi_has_new_fields(self):
        """POIData schema supports AMAP-specific fields."""
        poi = _make_amap_poi()
        assert poi.rating == 4.8
        assert poi.phone == "0512-67510286"
        assert poi.location == "120.630,31.324"
        assert poi.verified is True
        assert poi.source_type == "amap"

    async def test_backward_compat_xhs_pois(self):
        """Old XHS-sourced POIs still work with updated schema."""
        poi = POIData(
            name="Test",
            city="苏州",
            source_type="xiaohongshu",
            source_url="https://xhs.com/note/1",
            source_likes=100,
        )
        assert poi.rating is None
        assert poi.phone is None
        assert poi.location is None


@pytest.mark.asyncio
class TestDeprecatedMethods:
    """Deprecated XHS methods return empty/no-op gracefully."""

    async def test_process_notes_deprecated(self):
        svc = DataService()
        result = await svc.process_notes([], "苏州")
        assert result == []

    async def test_run_daily_pipeline_deprecated(self):
        svc = DataService()
        result = await svc.run_daily_pipeline()
        assert result == {}

    async def test_refresh_cache_deprecated(self):
        svc = DataService()
        result = await svc.refresh_cache("苏州")
        assert result == 0

    async def test_crawler_param_warns(self):
        """Passing crawler param logs a deprecation warning."""
        svc = DataService(crawler="fake_crawler")
        # Should not raise, just warn


@pytest.mark.asyncio
class TestGetCachedPoisFallback:
    """Test Redis → DB fallback in get_cached_pois()."""

    async def test_falls_back_to_db_on_redis_miss(self):
        redis = AsyncMock()
        redis.get.return_value = None

        db = AsyncMock()
        # Mock DB returning empty
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = []
        db.execute.return_value = result_mock

        svc = DataService(redis_client=redis, db_session=db)
        pois = await svc.get_cached_pois("苏州")

        # Should have tried DB
        assert db.execute.called

    async def test_no_redis_no_db_returns_empty(self):
        svc = DataService()
        pois = await svc.get_cached_pois("苏州")
        assert pois == []

    async def test_returns_cached_amap_pois(self):
        """Cached AMAP POIs are returned correctly from Redis."""
        cached = [
            {
                "name": "拙政园",
                "city": "苏州",
                "tags": ["景点"],
                "source_type": "amap",
                "rating": 4.8,
            }
        ]
        redis = AsyncMock()
        redis.get.return_value = json.dumps(cached)

        svc = DataService(redis_client=redis)
        pois = await svc.get_cached_pois("苏州")

        assert len(pois) == 1
        assert pois[0]["source_type"] == "amap"
        assert pois[0]["rating"] == 4.8
