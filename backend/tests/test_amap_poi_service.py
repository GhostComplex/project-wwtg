"""Tests for AmapPoiService."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.amap_poi_service import AmapPoiService, _map_type_to_tags


@pytest.mark.asyncio
class TestAmapPoiService:
    """Test AMAP POI search service."""

    async def test_mock_search_without_api_key(self):
        """Without API key, returns mock data."""
        svc = AmapPoiService(api_key="")
        results = await svc.search_text(city="苏州", types="110000")
        assert len(results) >= 1
        assert results[0]["name"]
        await svc.close()

    async def test_parse_poi_full_data(self):
        """_parse_poi correctly maps all fields."""
        raw = {
            "name": "拙政园",
            "address": "东北街178号",
            "location": "120.630,31.324",
            "type": "风景名胜;公园",
            "biz_ext": {"rating": "4.8"},
            "tel": "0512-67510286",
        }
        parsed = AmapPoiService._parse_poi(raw)
        assert parsed["name"] == "拙政园"
        assert parsed["address"] == "东北街178号"
        assert parsed["location"] == "120.630,31.324"
        assert parsed["rating"] == 4.8
        assert parsed["phone"] == "0512-67510286"
        assert "景点" in parsed["tags"]

    async def test_parse_poi_missing_rating(self):
        """Missing/empty rating is None."""
        raw = {
            "name": "Test",
            "biz_ext": {"rating": "[]"},
            "tel": "[]",
            "type": "",
        }
        parsed = AmapPoiService._parse_poi(raw)
        assert parsed["rating"] is None
        assert parsed["phone"] == ""

    async def test_parse_poi_no_biz_ext(self):
        """No biz_ext at all."""
        raw = {"name": "Test", "type": "餐饮服务;中餐厅"}
        parsed = AmapPoiService._parse_poi(raw)
        assert parsed["rating"] is None
        assert "美食" in parsed["tags"]

    async def test_fetch_city_deduplicates(self):
        """fetch_city_pois deduplicates by name."""
        svc = AmapPoiService(api_key="test")

        # Mock search_text to return overlapping results
        call_count = 0

        async def mock_search(city, types, page, **kw):
            nonlocal call_count
            call_count += 1
            if page > 1:
                return []  # Only 1 page of results
            return [
                {"name": "拙政园", "address": "addr1", "location": "120,31",
                 "amap_type": "风景名胜", "tags": ["景点"], "rating": 4.8, "phone": ""},
                {"name": "苏州博物馆", "address": "addr2", "location": "120,31",
                 "amap_type": "博物馆", "tags": ["文化"], "rating": 4.9, "phone": ""},
            ]

        svc.search_text = mock_search

        # Two type codes that return same POIs
        results = await svc.fetch_city_pois("苏州", {"景点": "110000", "博物馆": "110201"}, pages=2)

        # Should deduplicate
        names = [r["name"] for r in results]
        assert len(names) == len(set(names))
        await svc.close()


class TestTypeMapping:
    """Test AMAP type to tag mapping."""

    def test_known_types(self):
        assert "景点" in _map_type_to_tags("风景名胜;公园")
        assert "美食" in _map_type_to_tags("餐饮服务;中餐厅")
        assert "咖啡" in _map_type_to_tags("咖啡厅")

    def test_unknown_type_returns_other(self):
        tags = _map_type_to_tags("未知分类")
        assert tags == ["其他"]

    def test_no_duplicate_tags(self):
        tags = _map_type_to_tags("公园广场;公园")
        assert len(tags) == len(set(tags))
