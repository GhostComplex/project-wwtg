"""Plan service: plan generation and storage with LRU detail cache."""

import logging
import uuid
from collections import OrderedDict
from typing import Any

from app.models.schemas import PlanCard, PlanDetail, PlanSource, PlanStop, UserContext
from app.services.llm_service import LLMService
from app.services.map_service import MapService

logger = logging.getLogger(__name__)

# Maximum number of PlanDetail objects to cache
_DETAIL_CACHE_MAX_SIZE: int = 100


class PlanService:
    """Generates and stores weekend plans."""

    def __init__(
        self,
        llm: LLMService | None = None,
        map_service: MapService | None = None,
    ) -> None:
        self.llm = llm
        self.map = map_service
        # In-memory plan store keyed by plan_id
        self._plans: dict[str, dict[str, Any]] = {}
        # LRU cache for PlanDetail objects (OrderedDict for O(1) move-to-end)
        self._detail_cache: OrderedDict[str, PlanDetail] = OrderedDict()

    async def generate_plans(
        self,
        context: dict[str, Any],
        weather: dict[str, Any],
        pois: list[dict[str, Any]],
        rejected_plans: list[str] | None = None,
    ) -> list[PlanCard]:
        """Generate 2 plans via LLM and return as PlanCards."""
        if self.llm is None:
            return self.generate_mock_plans(UserContext(**{k: v for k, v in context.items() if v is not None and k in UserContext.model_fields}))

        # Determine if using AI-generated data (no real crawler sources)
        all_ai_generated = not pois or all(
            p.get("source_type") == "ai_generated" for p in pois
        )

        raw_plans = await self.llm.generate_plans(context, weather, pois, rejected_plans)

        cards: list[PlanCard] = []
        for plan_data in raw_plans[:2]:
            plan_id = plan_data.get("plan_id") or f"plan-{uuid.uuid4().hex[:8]}"
            plan_data["plan_id"] = plan_id

            # If all POIs are AI-generated, mark plans accordingly
            if all_ai_generated:
                plan_data["source_type"] = "ai_generated"
                # Remove xiaohongshu source references
                plan_data.pop("sources", None)
                plan_data["sources"] = []

            # Store full plan for detail retrieval
            self._plans[plan_id] = plan_data

            card = PlanCard(
                plan_id=plan_id,
                title=plan_data.get("title", "周末方案"),
                emoji=plan_data.get("emoji", "📍"),
                description=plan_data.get("description", ""),
                duration=plan_data.get("duration", "半天"),
                cost_range=plan_data.get("cost_range", ""),
                transport=plan_data.get("transport", "步行"),
                tags=plan_data.get("tags", []),
                stops_count=plan_data.get("stops_count", len(plan_data.get("stops", []))),
                source_count=plan_data.get("source_count", len(plan_data.get("sources", []))),
            )
            cards.append(card)

        # Enrich plans with navigation links asynchronously
        city = context.get("city", "苏州")
        for plan_data in [self._plans[c.plan_id] for c in cards]:
            await self._enrich_stops_with_nav(plan_data, city)

        return cards

    async def _enrich_stops_with_nav(self, plan_data: dict[str, Any], city: str) -> None:
        """Geocode stops and add navigation links + walking distances."""
        if self.map is None:
            return

        stops = plan_data.get("stops", [])
        geocoded: list[dict[str, Any] | None] = []

        for stop in stops:
            if stop.get("nav_link"):
                # Already has a nav link, try to extract coords for distance calc
                geocoded.append(None)
                continue
            try:
                geo = await self.map.geocode(stop.get("name", ""), city)
                if geo:
                    stop["nav_link"] = self.map.generate_nav_link(
                        stop["name"], geo["lat"], geo["lng"]
                    )
                    geocoded.append(geo)
                else:
                    geocoded.append(None)
            except Exception:
                logger.warning("Geocode failed for stop: %s", stop.get("name"))
                geocoded.append(None)

        # Calculate walking distances between consecutive stops
        for i in range(len(stops) - 1):
            if stops[i].get("walk_to_next"):
                continue  # Already has distance
            geo_a = geocoded[i] if i < len(geocoded) else None
            geo_b = geocoded[i + 1] if (i + 1) < len(geocoded) else None
            if geo_a and geo_b:
                try:
                    walk = await self.map.calculate_walking_distance(
                        (geo_a["lng"], geo_a["lat"]),
                        (geo_b["lng"], geo_b["lat"]),
                    )
                    stops[i]["walk_to_next"] = f"{walk['distance']}, {walk['duration']}"
                except Exception:
                    logger.warning("Walking distance calc failed: %s → %s",
                                   stops[i].get("name"), stops[i + 1].get("name"))

    async def get_plan_detail(self, plan_id: str) -> PlanDetail:
        """Get full plan detail, enriched with nav links.

        Uses an in-memory LRU cache (maxsize=100) to avoid
        re-building PlanDetail from raw data on repeated lookups.
        """
        # Check LRU cache first
        if plan_id in self._detail_cache:
            logger.debug("Plan detail cache HIT for %s", plan_id)
            # Move to end (most recently used)
            self._detail_cache.move_to_end(plan_id)
            return self._detail_cache[plan_id]

        logger.debug("Plan detail cache MISS for %s", plan_id)

        # Build from raw data or fallback
        plan_data = self._plans.get(plan_id)
        if plan_data:
            detail = self._plan_data_to_detail(plan_data)
        else:
            detail = self.get_mock_detail(plan_id)

        # Store in LRU cache, evict oldest if full
        self._detail_cache[plan_id] = detail
        if len(self._detail_cache) > _DETAIL_CACHE_MAX_SIZE:
            evicted_key, _ = self._detail_cache.popitem(last=False)
            logger.debug("Plan detail cache evicted %s (maxsize=%d)", evicted_key, _DETAIL_CACHE_MAX_SIZE)

        return detail

    def _plan_data_to_detail(self, plan_data: dict[str, Any]) -> PlanDetail:
        """Convert raw plan data dict to PlanDetail model."""
        stops = []
        for s in plan_data.get("stops", []):
            nav_link = s.get("nav_link", "")
            # If we have map service and no nav_link, we could geocode here
            # but that's async — for now use what we have
            stops.append(PlanStop(
                name=s.get("name", ""),
                arrive_at=s.get("arrive_at", ""),
                stay_duration=s.get("stay_duration", ""),
                recommendation=s.get("recommendation", ""),
                nav_link=nav_link,
                walk_to_next=s.get("walk_to_next", ""),
            ))

        sources = []
        for src in plan_data.get("sources", []):
            sources.append(PlanSource(
                title=src.get("title", ""),
                likes=src.get("likes", 0),
                url=src.get("url", ""),
            ))

        return PlanDetail(
            plan_id=plan_data.get("plan_id", ""),
            title=plan_data.get("title", ""),
            stops=stops,
            tips=plan_data.get("tips", []),
            sources=sources,
        )

    # ------------------------------------------------------------------
    # Mock helpers (kept for backward compat)
    # ------------------------------------------------------------------

    def generate_mock_plans(self, ctx: UserContext) -> list[PlanCard]:
        """Return 2 mock plans and store them for detail retrieval."""
        city = ctx.city or "苏州"
        tags_a = ["免费", "有餐饮"]
        tags_b = ["室内为主", "文艺", "免费"]
        if "孕妇" in ctx.constraints:
            tags_a.insert(0, "孕妇友好")
            tags_b.append("孕妇友好")
        if "人少" in ctx.preferences:
            tags_a.insert(0, "人少")

        plan_a_id = f"plan-{uuid.uuid4().hex[:8]}"
        plan_b_id = f"plan-{uuid.uuid4().hex[:8]}"

        # Store full data
        self._plans[plan_a_id] = {
            "plan_id": plan_a_id,
            "title": "双塔市集赏花散步",
            "emoji": "🌸",
            "description": "玉兰花季，吃喝逛一条线",
            "duration": "半天（3-4小时）",
            "cost_range": "50元以内",
            "transport": "地铁+步行",
            "tags": tags_a,
            "source_type": "xiaohongshu",
            "stops": [
                {"name": "双塔市集", "arrive_at": "10:00", "stay_duration": "30-45分钟",
                 "recommendation": "老客满蛋饼 + 冰豆浆",
                 "nav_link": "https://uri.amap.com/marker?position=120.636,31.316&name=%E5%8F%8C%E5%A1%94%E5%B8%82%E9%9B%86",
                 "walk_to_next": "240m, 约3分钟"},
                {"name": "定慧寺巷", "arrive_at": "10:45", "stay_duration": "20分钟",
                 "recommendation": "玉兰花拍照打卡", "nav_link": "", "walk_to_next": "500m, 约6分钟"},
                {"name": "耦园", "arrive_at": "11:15", "stay_duration": "45-60分钟",
                 "recommendation": "世界文化遗产，人少清净",
                 "nav_link": "https://uri.amap.com/marker?position=120.643,31.318&name=%E8%80%A6%E5%9B%AD",
                 "walk_to_next": "300m, 约4分钟"},
                {"name": "相门城墙", "arrive_at": "12:15", "stay_duration": "30分钟",
                 "recommendation": "城墙上散步看护城河", "nav_link": "", "walk_to_next": ""},
            ],
            "tips": ["明天 7-15°C 多云，建议穿薄外套", "全程步行约 1.6km，平路为主，孕妇友好", "双塔市集周一休市，请注意时间"],
            "sources": [
                {"title": "苏州赏花路线合集", "likes": 2340, "url": "https://example.com/1"},
                {"title": "双塔市集必吃攻略", "likes": 1820, "url": "https://example.com/2"},
                {"title": "苏州半日游路线", "likes": 956, "url": "https://example.com/3"},
            ],
        }

        self._plans[plan_b_id] = {
            "plan_id": plan_b_id,
            "title": f"{city}博物馆文艺之旅",
            "emoji": "🏛️",
            "description": "看展逛馆，咖啡收尾",
            "duration": "半天（3-4小时）",
            "cost_range": "30元以内",
            "transport": "地铁+步行",
            "tags": tags_b,
            "source_type": "ai_generated",
            "stops": [
                {"name": "苏州博物馆", "arrive_at": "09:30", "stay_duration": "1.5-2小时",
                 "recommendation": "贝聿铭设计，免费预约",
                 "nav_link": "https://uri.amap.com/marker?position=120.628,31.322&name=%E8%8B%8F%E5%B7%9E%E5%8D%9A%E7%89%A9%E9%A6%86",
                 "walk_to_next": "800m, 约10分钟"},
                {"name": "平江路", "arrive_at": "11:30", "stay_duration": "1小时",
                 "recommendation": "逛小店 + 午餐", "nav_link": "", "walk_to_next": "200m, 约2分钟"},
                {"name": "猫的天空之城", "arrive_at": "12:30", "stay_duration": "30-45分钟",
                 "recommendation": "写明信片 + 咖啡", "nav_link": "", "walk_to_next": ""},
            ],
            "tips": ["苏州博物馆需提前预约，免费", "平江路人流较大，建议避开下午高峰"],
            "sources": [
                {"title": "苏州博物馆攻略", "likes": 3100, "url": "https://example.com/4"},
                {"title": "平江路美食地图", "likes": 1560, "url": "https://example.com/5"},
            ],
        }

        plan_a = PlanCard(
            plan_id=plan_a_id, title="双塔市集赏花散步", emoji="🌸",
            description="玉兰花季，吃喝逛一条线", duration="半天（3-4小时）",
            cost_range="50元以内", transport="地铁+步行", tags=tags_a,
            stops_count=4, source_count=3,
        )
        plan_b = PlanCard(
            plan_id=plan_b_id, title=f"{city}博物馆文艺之旅", emoji="🏛️",
            description="看展逛馆，咖啡收尾", duration="半天（3-4小时）",
            cost_range="30元以内", transport="地铁+步行", tags=tags_b,
            stops_count=3, source_count=2,
        )
        return [plan_a, plan_b]

    def get_mock_detail(self, plan_id: str) -> PlanDetail:
        """Return mock plan detail (fallback)."""
        # Check stored plans first
        if plan_id in self._plans:
            return self._plan_data_to_detail(self._plans[plan_id])

        # Hardcoded fallback
        return PlanDetail(
            plan_id=plan_id,
            title="周末方案",
            stops=[
                PlanStop(name="起点", arrive_at="10:00", stay_duration="1小时",
                         recommendation="探索周边", nav_link="", walk_to_next=""),
            ],
            tips=["请关注天气变化"],
            sources=[],
        )
