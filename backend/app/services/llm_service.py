"""LLM client: DeepSeek V3 integration with mock fallback."""

import json
import logging
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

# --- System Prompts ---

PARSE_INTENT_SYSTEM = """你是"周末搭子"的意图解析模块。用户会告诉你他们的周末出行需求。
请从用户消息中提取以下信息，返回JSON格式：

{
  "city": "城市名（如苏州、上海、杭州）或null",
  "people_count": 人数（整数）或null,
  "companion_type": "独自/情侣/亲子/朋友/家人"或null,
  "energy_level": "high/medium/low"或null,
  "constraints": ["约束条件列表，如孕妇、轮椅、老人等"],
  "preferences": ["偏好列表，如人少、免费、拍照、美食等"]
}

注意：
- 用户可能一句话包含多个信息，如"苏州，和老公，我是孕妇"
- "老公/老婆"→情侣，"孩子/娃/宝宝"→亲子，"朋友/闺蜜/哥们"→朋友
- "孕妇"是constraint不是companion_type
- 没提到的字段设为null或空列表
- 只返回JSON，不要其他文字"""

GENERATE_PLANS_SYSTEM = """你是"周末搭子"，像一个靠谱的本地朋友帮人规划周末出行。
根据用户需求、天气和POI数据，生成2个差异化方案（一动一静 或 一远一近）。

每个方案必须包含以下字段：
{
  "plan_id": "生成唯一ID",
  "title": "方案标题（简短有趣）",
  "emoji": "一个代表方案的emoji",
  "description": "一句话描述",
  "duration": "预计时长",
  "cost_range": "预估花费",
  "transport": "交通方式",
  "tags": ["标签列表"],
  "stops_count": 站点数量,
  "source_count": 数据来源数,
  "stops": [
    {
      "name": "地点名",
      "arrive_at": "到达时间",
      "stay_duration": "停留时长",
      "recommendation": "推荐理由/玩法",
      "walk_to_next": "到下一站的步行距离和时间"
    }
  ],
  "tips": ["贴心提示列表"],
  "sources": [{"title": "来源标题", "likes": 点赞数, "url": "链接"}]
}

要求：
- 方案A偏活力/户外，方案B偏安静/休闲
- 考虑天气因素（下雨→室内方案）
- 考虑用户约束（孕妇→避免爬山、长距离步行）
- 每个方案3-5个站点，路线合理（步行可达或短途交通）
- 返回JSON数组 [方案A, 方案B]
- 如果有被拒绝的方案，避免类似推荐
- **严禁编造不存在的地点和店铺。只能使用提供的POI数据中提到的地点。如果数据不够就减少站点数量，不要凑数。**
- **优先使用标记为 verified:true 的POI（已通过高德地图验证的真实地点）。**
- sources字段必须引用提供的真实笔记数据
- 每个stop必须标注数据来源（来自哪条笔记）"""

EXTRACT_POIS_SYSTEM = """从小红书笔记中提取结构化的POI（兴趣点）数据。
笔记可能只有标题没有正文，请从标题、标签、点赞数等信息推断地点。
对每组笔记，提取其中提到或暗示的具体地点（景点、餐厅、咖啡馆、公园等），返回JSON数组：
[{
  "name": "地点名称",
  "address": "地址（如有，没有则为null）",
  "tags": ["标签"],
  "description": "简短描述（基于笔记内容推断）",
  "cost_range": "花费范围（如有，没有则为null）",
  "suitable_for": ["适合人群"]
}]
如果无法提取任何具体地点，返回空数组 []。只返回JSON。"""

GENERATE_POI_RECOMMENDATIONS_SYSTEM = """你是"周末搭子"的内容生成模块。根据高德地图POI元数据，为每个地点生成推荐理由和标签。

要求：
- tags：3-5个场景标签（如"孕妇友好""免费""花季推荐""遛娃""拍照""安静"）
- reason：50字以内口语化推荐理由，像朋友推荐一样自然
- suitable_for：适合人群（独自/情侣/亲子/朋友/家人，可多选）
- cost_range：花费区间（免费/30以内/50以内/50-100/100+）

输入是JSON数组，每个元素有name/type/rating/address/city字段。
输出也是JSON数组，顺序和输入一一对应，每个元素：
{
  "tags": ["标签1", "标签2", ...],
  "reason": "口语化推荐理由",
  "suitable_for": ["适合人群"],
  "cost_range": "花费区间"
}

注意：
- 公园、博物馆通常免费
- 根据地点类型推断适合人群（游乐园→亲子，咖啡厅→情侣/朋友，公园→全人群）
- reason要有信息量，不要泛泛而谈
- 只返回JSON数组，不要其他文字"""


class LLMService:
    """DeepSeek V3 LLM client with mock fallback."""

    def __init__(
        self,
        api_key: str = "",
        base_url: str = "https://api.deepseek.com",
        model: str = "deepseek-chat",
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=httpx.Timeout(settings.llm_timeout, connect=10.0),
            )
        return self._client

    async def chat_completion(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = 1000,
        retries: int = 2,
    ) -> str:
        """Call DeepSeek API with retry. Returns raw content string."""
        if not self.api_key:
            raise ValueError("No API key configured")

        client = await self._get_client()
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.7,
            "response_format": {"type": "json_object"},
        }

        last_err: Exception | None = None
        for attempt in range(retries + 1):
            try:
                resp = await client.post("/chat/completions", json=payload)
                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"]
            except Exception as e:
                last_err = e
                logger.warning("LLM API attempt %d failed: %s", attempt + 1, e)
                if attempt < retries:
                    import asyncio
                    await asyncio.sleep(1 * (attempt + 1))

        raise last_err  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def parse_intent(
        self, user_message: str, conversation_history: list[dict[str, str]] | None = None
    ) -> dict[str, Any]:
        """Extract user context from message."""
        if not self.api_key:
            logger.warning("LLM_API_KEY not set, using mock parse_intent")
            return self._mock_parse_intent(user_message)

        history_text = ""
        if conversation_history:
            history_text = "\n之前的对话：\n" + "\n".join(
                f"{m['role']}: {m['content']}" for m in conversation_history[-6:]
            )

        prompt = f"{history_text}\n用户最新消息：{user_message}"
        try:
            raw = await self.chat_completion(PARSE_INTENT_SYSTEM, prompt, max_tokens=500)
            return json.loads(raw)
        except Exception as e:
            logger.error("parse_intent failed, falling back to mock: %s", e)
            return self._mock_parse_intent(user_message)

    async def generate_plans(
        self,
        context: dict[str, Any],
        weather: dict[str, Any],
        pois: list[dict[str, Any]],
        rejected_plans: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Generate 2 differentiated plans."""
        if not self.api_key:
            logger.warning("LLM_API_KEY not set, using mock generate_plans")
            return self._mock_generate_plans(context)

        prompt_parts = [
            f"用户需求：{json.dumps(context, ensure_ascii=False)}",
            f"天气：{json.dumps(weather, ensure_ascii=False)}",
        ]
        if pois:
            prompt_parts.append(f"可用POI（前10个）：{json.dumps(pois[:10], ensure_ascii=False)}")
        if rejected_plans:
            prompt_parts.append(f"已拒绝的方案标题（请避免类似推荐）：{rejected_plans}")

        try:
            raw = await self.chat_completion(
                GENERATE_PLANS_SYSTEM, "\n".join(prompt_parts), max_tokens=4000
            )
            result = json.loads(raw)
            # Log plan structure for debugging
            plans_list = result if isinstance(result, list) else result.get("plans", [result])
            for i, p in enumerate(plans_list):
                if isinstance(p, dict):
                    logger.warning("Plan %d has %d stops", i, len(p.get("stops", [])))
            # Handle both {"plans": [...]} and [...] formats
            if isinstance(result, dict) and "plans" in result:
                return result["plans"]
            if isinstance(result, list):
                return result
            return [result]
        except Exception as e:
            logger.error("generate_plans failed, falling back to mock: %s", e)
            return self._mock_generate_plans(context)

    async def extract_pois(self, notes: list[dict[str, Any]], city: str) -> list[dict[str, Any]]:
        """Extract structured POI data from crawled notes."""
        if not self.api_key:
            return []

        notes_text = json.dumps(
            [
                {
                    "title": n.get("title", ""),
                    "content": n.get("content", "")[:200] or "(无正文)",
                    "tags": n.get("tags", []),
                    "likes": n.get("likes", 0),
                    "url": n.get("url", ""),
                }
                for n in notes[:5]
            ],
            ensure_ascii=False,
        )
        try:
            raw = await self.chat_completion(
                EXTRACT_POIS_SYSTEM,
                f"城市：{city}\n笔记数据：{notes_text}",
                max_tokens=1500,
            )
            result = json.loads(raw)
            if isinstance(result, dict) and "pois" in result:
                return result["pois"]
            if isinstance(result, list):
                return result
            return []
        except Exception as e:
            logger.error("extract_pois failed: %s", e)
            return []

    async def generate_poi_recommendations(
        self,
        pois: list[dict[str, Any]],
        season: str = "",
        batch_size: int = 10,
    ) -> list[dict[str, Any]]:
        """Generate tags, reason, suitable_for, cost_range for a batch of POIs.

        Processes POIs in batches to reduce LLM call count.
        Results are returned in the same order as input.

        Args:
            pois: List of POI dicts with keys: name, amap_type/type, rating, address, city.
            season: Current season hint (e.g. "春天", "夏天").
            batch_size: Number of POIs per LLM call.

        Returns:
            List of dicts with keys: tags, reason, suitable_for, cost_range.
            Same length as input; failed items get mock defaults.
        """
        if not self.api_key:
            logger.warning("No LLM API key, using mock recommendations")
            return [self._mock_recommendation(p) for p in pois]

        results: list[dict[str, Any]] = []

        for i in range(0, len(pois), batch_size):
            batch = pois[i : i + batch_size]
            batch_input = [
                {
                    "name": p.get("name", ""),
                    "type": p.get("amap_type", p.get("type", "")),
                    "rating": p.get("rating"),
                    "address": p.get("address", ""),
                    "city": p.get("city", ""),
                }
                for p in batch
            ]

            season_hint = f"\n当前季节：{season}" if season else ""
            prompt = (
                f"为以下{len(batch)}个地点生成推荐信息：{season_hint}\n"
                f"{json.dumps(batch_input, ensure_ascii=False)}"
            )

            try:
                raw = await self.chat_completion(
                    GENERATE_POI_RECOMMENDATIONS_SYSTEM,
                    prompt,
                    max_tokens=200 * len(batch),  # ~200 tokens per POI
                )
                parsed = json.loads(raw)

                # Handle both list and {"recommendations": [...]} formats
                if isinstance(parsed, dict):
                    parsed = parsed.get("recommendations", parsed.get("results", []))
                if not isinstance(parsed, list):
                    parsed = [parsed]

                # Pad or trim to match batch size
                for j, poi in enumerate(batch):
                    if j < len(parsed) and isinstance(parsed[j], dict):
                        rec = parsed[j]
                        results.append({
                            "tags": rec.get("tags", [])[:5],
                            "reason": (rec.get("reason", "") or "")[:100],
                            "suitable_for": rec.get("suitable_for", []),
                            "cost_range": rec.get("cost_range", ""),
                        })
                    else:
                        results.append(self._mock_recommendation(poi))

                logger.info(
                    "LLM generated recommendations for batch %d (%d POIs)",
                    i // batch_size + 1, len(batch),
                )
            except Exception:
                logger.exception(
                    "LLM recommendation failed for batch %d, using mock",
                    i // batch_size + 1,
                )
                results.extend(self._mock_recommendation(p) for p in batch)

        return results

    @staticmethod
    def _mock_recommendation(poi: dict[str, Any]) -> dict[str, Any]:
        """Generate a basic recommendation from POI metadata without LLM."""
        amap_type = poi.get("amap_type", poi.get("type", ""))
        name = poi.get("name", "")

        # Infer cost from type
        free_types = ("公园", "博物馆", "纪念馆", "广场", "风景名胜")
        is_free = any(t in amap_type for t in free_types)

        # Infer suitable_for from type
        suitable = ["朋友"]
        if any(t in amap_type for t in ("亲子", "儿童", "游乐")):
            suitable = ["亲子", "家人"]
        elif any(t in amap_type for t in ("咖啡", "甜品", "餐饮")):
            suitable = ["情侣", "朋友"]
        elif any(t in amap_type for t in ("公园", "广场", "风景")):
            suitable = ["独自", "情侣", "亲子", "朋友", "家人"]

        return {
            "tags": [],
            "reason": f"{name}，值得一去",
            "suitable_for": suitable,
            "cost_range": "免费" if is_free else "",
        }

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Mock fallbacks
    # ------------------------------------------------------------------

    def _mock_parse_intent(self, message: str) -> dict[str, Any]:
        """Keyword-based intent parsing fallback."""
        result: dict[str, Any] = {
            "city": None,
            "people_count": None,
            "companion_type": None,
            "energy_level": None,
            "constraints": [],
            "preferences": [],
        }

        cities = ["苏州", "上海", "杭州", "南京", "北京", "广州", "深圳", "成都"]
        for city in cities:
            if city in message:
                result["city"] = city
                break

        if "老公" in message or "老婆" in message or "情侣" in message:
            result["companion_type"] = "情侣"
        elif "孩子" in message or "亲子" in message or "娃" in message:
            result["companion_type"] = "亲子"
        elif "朋友" in message or "闺蜜" in message:
            result["companion_type"] = "朋友"

        if "孕妇" in message:
            result["constraints"].append("孕妇")
        if "轮椅" in message:
            result["constraints"].append("轮椅")
        if "人少" in message:
            result["preferences"].append("人少")
        if "免费" in message:
            result["preferences"].append("免费")

        result["_source"] = "mock"
        return result

    def _mock_generate_plans(self, context: dict[str, Any]) -> list[dict[str, Any]]:
        """Return mock plans."""
        import uuid

        city = context.get("city", "苏州")
        constraints = context.get("constraints", [])
        tags_a = ["免费", "有餐饮"]
        tags_b = ["室内为主", "文艺", "免费"]
        if "孕妇" in constraints:
            tags_a.insert(0, "孕妇友好")
            tags_b.append("孕妇友好")

        plan_a = {
            "plan_id": f"plan-{uuid.uuid4().hex[:8]}",
            "title": "双塔市集赏花散步",
            "emoji": "🌸",
            "description": "玉兰花季，吃喝逛一条线",
            "duration": "半天（3-4小时）",
            "cost_range": "50元以内",
            "transport": "地铁+步行",
            "tags": tags_a,
            "stops_count": 4,
            "source_count": 3,
            "source_type": "xiaohongshu",
            "stops": [
                {"name": "双塔市集", "arrive_at": "10:00", "stay_duration": "30-45分钟",
                 "recommendation": "老客满蛋饼 + 冰豆浆", "walk_to_next": "240m, 约3分钟"},
                {"name": "定慧寺巷", "arrive_at": "10:45", "stay_duration": "20分钟",
                 "recommendation": "玉兰花拍照打卡", "walk_to_next": "500m, 约6分钟"},
                {"name": "耦园", "arrive_at": "11:15", "stay_duration": "45-60分钟",
                 "recommendation": "世界文化遗产，人少清净", "walk_to_next": "300m, 约4分钟"},
                {"name": "相门城墙", "arrive_at": "12:15", "stay_duration": "30分钟",
                 "recommendation": "城墙上散步看护城河", "walk_to_next": ""},
            ],
            "tips": ["明天 7-15°C 多云，建议穿薄外套", "全程步行约 1.6km，平路为主，孕妇友好"],
            "sources": [
                {"title": "苏州赏花路线合集", "likes": 2340, "url": "https://example.com/1"},
                {"title": "双塔市集必吃攻略", "likes": 1820, "url": "https://example.com/2"},
            ],
        }

        plan_b = {
            "plan_id": f"plan-{uuid.uuid4().hex[:8]}",
            "title": f"{city}博物馆文艺之旅",
            "emoji": "🏛️",
            "description": "看展逛馆，咖啡收尾",
            "duration": "半天（3-4小时）",
            "cost_range": "30元以内",
            "transport": "地铁+步行",
            "tags": tags_b,
            "stops_count": 3,
            "source_count": 2,
            "source_type": "ai_generated",
            "stops": [
                {"name": "苏州博物馆", "arrive_at": "09:30", "stay_duration": "1.5-2小时",
                 "recommendation": "贝聿铭设计，免费预约", "walk_to_next": "800m, 约10分钟"},
                {"name": "平江路", "arrive_at": "11:30", "stay_duration": "1小时",
                 "recommendation": "逛小店 + 午餐", "walk_to_next": "200m, 约2分钟"},
                {"name": "猫的天空之城", "arrive_at": "12:30", "stay_duration": "30-45分钟",
                 "recommendation": "写明信片 + 咖啡", "walk_to_next": ""},
            ],
            "tips": ["苏州博物馆需提前预约，免费", "平江路人流较大，建议避开下午高峰"],
            "sources": [
                {"title": "苏州博物馆攻略", "likes": 3100, "url": "https://example.com/4"},
            ],
        }

        plan_a["_source"] = "mock"
        plan_b["_source"] = "mock"
        return [plan_a, plan_b]
