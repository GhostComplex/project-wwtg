"""Tests for LLM service."""

import pytest

from app.services.llm_service import LLMService


@pytest.fixture
def llm_no_key() -> LLMService:
    return LLMService(api_key="")


@pytest.fixture
def llm_with_key() -> LLMService:
    return LLMService(api_key="test-key-123")


class TestParseIntentMock:
    """Test mock parse_intent (no API key)."""

    @pytest.mark.asyncio
    async def test_single_city(self, llm_no_key: LLMService) -> None:
        result = await llm_no_key.parse_intent("苏州", [])
        assert result["city"] == "苏州"

    @pytest.mark.asyncio
    async def test_multi_info(self, llm_no_key: LLMService) -> None:
        result = await llm_no_key.parse_intent("苏州，和老公，我是孕妇", [])
        assert result["city"] == "苏州"
        assert result["companion_type"] == "情侣"
        assert "孕妇" in result["constraints"]

    @pytest.mark.asyncio
    async def test_preferences(self, llm_no_key: LLMService) -> None:
        result = await llm_no_key.parse_intent("人少免费的地方", [])
        assert "人少" in result["preferences"]
        assert "免费" in result["preferences"]

    @pytest.mark.asyncio
    async def test_no_info(self, llm_no_key: LLMService) -> None:
        result = await llm_no_key.parse_intent("你好", [])
        assert result["city"] is None
        assert result["companion_type"] is None

    @pytest.mark.asyncio
    async def test_friend_companion(self, llm_no_key: LLMService) -> None:
        result = await llm_no_key.parse_intent("和朋友去杭州", [])
        assert result["city"] == "杭州"
        assert result["companion_type"] == "朋友"

    @pytest.mark.asyncio
    async def test_parent_child(self, llm_no_key: LLMService) -> None:
        result = await llm_no_key.parse_intent("带孩子去上海", [])
        assert result["city"] == "上海"
        assert result["companion_type"] == "亲子"


class TestGeneratePlansMock:
    """Test mock plan generation."""

    @pytest.mark.asyncio
    async def test_returns_two_plans(self, llm_no_key: LLMService) -> None:
        plans = await llm_no_key.generate_plans(
            context={"city": "苏州", "constraints": []},
            weather={"city": "苏州", "condition": "晴"},
            pois=[],
        )
        assert len(plans) == 2

    @pytest.mark.asyncio
    async def test_plan_has_required_fields(self, llm_no_key: LLMService) -> None:
        plans = await llm_no_key.generate_plans(
            context={"city": "苏州", "constraints": []},
            weather={},
            pois=[],
        )
        for plan in plans:
            assert "plan_id" in plan
            assert "title" in plan
            assert "emoji" in plan
            assert "stops" in plan
            assert "tips" in plan

    @pytest.mark.asyncio
    async def test_plans_with_constraints(self, llm_no_key: LLMService) -> None:
        plans = await llm_no_key.generate_plans(
            context={"city": "苏州", "constraints": ["孕妇"]},
            weather={},
            pois=[],
        )
        tags_all = []
        for p in plans:
            tags_all.extend(p.get("tags", []))
        assert "孕妇友好" in tags_all


class TestFallback:
    """Test that API failure falls back to mock."""

    @pytest.mark.asyncio
    async def test_parse_intent_fallback(self, llm_with_key: LLMService) -> None:
        # With a fake key, API call will fail and fall back to mock
        result = await llm_with_key.parse_intent("苏州", [])
        assert result["city"] == "苏州"

    @pytest.mark.asyncio
    async def test_generate_plans_fallback(self, llm_with_key: LLMService) -> None:
        plans = await llm_with_key.generate_plans(
            context={"city": "苏州", "constraints": []},
            weather={},
            pois=[],
        )
        assert len(plans) == 2


class TestGeneratePoiRecommendations:
    """Test LLM-based POI recommendation generation (M2)."""

    @pytest.mark.asyncio
    async def test_mock_recommendations_without_api_key(self, llm_no_key: LLMService) -> None:
        """Without API key, returns mock recommendations."""
        pois = [
            {"name": "拙政园", "amap_type": "风景名胜;公园", "rating": 4.8, "address": "东北街178号"},
            {"name": "星巴克", "amap_type": "餐饮服务;咖啡厅", "rating": 4.2, "address": "观前街1号"},
        ]
        results = await llm_no_key.generate_poi_recommendations(pois)
        assert len(results) == 2
        # Each result should have the required keys
        for r in results:
            assert "tags" in r
            assert "reason" in r
            assert "suitable_for" in r
            assert "cost_range" in r

    @pytest.mark.asyncio
    async def test_mock_infers_free_for_parks(self, llm_no_key: LLMService) -> None:
        """Mock correctly infers 'free' for park-type POIs."""
        pois = [{"name": "虎丘公园", "amap_type": "公园广场;公园", "rating": 4.5}]
        results = await llm_no_key.generate_poi_recommendations(pois)
        assert results[0]["cost_range"] == "免费"

    @pytest.mark.asyncio
    async def test_mock_infers_suitable_for_kids(self, llm_no_key: LLMService) -> None:
        """Mock correctly infers kid-friendly for child-type POIs."""
        pois = [{"name": "儿童乐园", "amap_type": "亲子乐园;儿童乐园", "rating": 4.0}]
        results = await llm_no_key.generate_poi_recommendations(pois)
        assert "亲子" in results[0]["suitable_for"]

    @pytest.mark.asyncio
    async def test_fallback_on_api_failure(self, llm_with_key: LLMService) -> None:
        """With fake API key, falls back to mock without crashing."""
        pois = [{"name": "Test", "amap_type": "风景名胜", "rating": 4.0}]
        results = await llm_with_key.generate_poi_recommendations(pois)
        assert len(results) == 1
        assert "reason" in results[0]

    @pytest.mark.asyncio
    async def test_batch_processing(self, llm_no_key: LLMService) -> None:
        """Many POIs are batched correctly."""
        pois = [{"name": f"地点{i}", "amap_type": "风景名胜"} for i in range(25)]
        results = await llm_no_key.generate_poi_recommendations(pois, batch_size=10)
        assert len(results) == 25

    @pytest.mark.asyncio
    async def test_season_param_accepted(self, llm_no_key: LLMService) -> None:
        """Season parameter is accepted without error."""
        pois = [{"name": "Test", "amap_type": "公园"}]
        results = await llm_no_key.generate_poi_recommendations(pois, season="春天")
        assert len(results) == 1
