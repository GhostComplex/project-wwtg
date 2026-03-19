"""Chat service: conversation state machine with LLM integration."""

import asyncio
import json
import logging
from typing import Any, Optional

from app.config import settings
from app.models.schemas import (
    ChatResponse,
    ConversationState,
    PlanCard,
    UserContext,
)
from app.services.analytics import analytics
from app.services.data_service import DataService
from app.services.llm_service import LLMService
from app.services.map_service import MapService
from app.services.plan_service import PlanService
from app.services.weather_service import WeatherService

logger = logging.getLogger(__name__)

# Redis key pattern and TTL for sessions
_SESSION_KEY = "wwtg:session:{session_id}"
_SESSION_TTL = 24 * 3600  # 24 hours


def _default_session() -> dict[str, Any]:
    """Create a fresh session dict."""
    return {
        "state": ConversationState.INIT.value,
        "context": UserContext().model_dump(),
        "history": [],
        "rejected_plans": [],
        "rejection_count": 0,
        "current_plans": [],
        "selected_plan": None,
    }


class ChatService:
    """Manages conversation flow and state transitions."""

    def __init__(self, redis_client: Any = None) -> None:
        self.llm = LLMService(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model=settings.llm_model,
        )
        self.weather = WeatherService(api_key=settings.amap_api_key)
        self.map = MapService(api_key=settings.amap_api_key)
        self.plan_service = PlanService(llm=self.llm, map_service=self.map)
        self.data = DataService(
            llm_service=self.llm,
            redis_client=redis_client,
        )

        self._redis = redis_client
        # In-memory fallback when Redis is unavailable
        self._sessions: dict[str, dict[str, Any]] = {}

    async def _get_session(self, session_id: str) -> dict[str, Any]:
        """Load session from Redis, falling back to in-memory."""
        # Try Redis first
        if self._redis is not None:
            key = _SESSION_KEY.format(session_id=session_id)
            try:
                raw = await self._redis.get(key)
                if raw:
                    data = json.loads(raw)
                    # Deserialize context back to UserContext
                    data["context"] = UserContext(**data["context"])
                    data["state"] = ConversationState(data["state"])
                    return data
            except Exception:
                logger.warning("Redis read failed for session %s, using in-memory", session_id)

        # In-memory fallback
        if session_id not in self._sessions:
            session = _default_session()
            session["context"] = UserContext()
            session["state"] = ConversationState.INIT
            self._sessions[session_id] = session
        return self._sessions[session_id]

    async def _save_session(self, session_id: str, session: dict[str, Any]) -> None:
        """Persist session to Redis (with in-memory fallback)."""
        # Always keep in-memory copy
        self._sessions[session_id] = session

        if self._redis is not None:
            key = _SESSION_KEY.format(session_id=session_id)
            try:
                # Serialize for Redis
                data = {
                    "state": session["state"].value if isinstance(session["state"], ConversationState) else session["state"],
                    "context": session["context"].model_dump() if hasattr(session["context"], "model_dump") else session["context"],
                    "history": session["history"],
                    "rejected_plans": session["rejected_plans"],
                    "rejection_count": session["rejection_count"],
                    "current_plans": [
                        p.model_dump() if hasattr(p, "model_dump") else p
                        for p in session.get("current_plans", [])
                    ],
                    "selected_plan": session.get("selected_plan"),
                }
                await self._redis.set(key, json.dumps(data, ensure_ascii=False), ex=_SESSION_TTL)
            except Exception:
                logger.warning("Redis write failed for session %s, in-memory only", session_id)

    async def get_history(self, session_id: str) -> list[dict[str, str]]:
        """Return conversation history for a session."""
        session = await self._get_session(session_id)
        return session.get("history", [])

    async def process_message(self, session_id: str, message: str) -> ChatResponse:
        """Process a user message through the full conversation flow."""
        session = await self._get_session(session_id)
        is_new = len(session["history"]) == 0
        state = session["state"]
        ctx: UserContext = session["context"]
        history: list[dict[str, str]] = session["history"]

        # Add user message to history
        history.append({"role": "user", "content": message})
        if is_new:
            await analytics.track("session_start", session_id=session_id)
        await analytics.track("message_sent", session_id=session_id,
                              properties={"message_length": len(message)})

        # --- Handle rejection / selection from PRESENTING state ---
        if state == ConversationState.PRESENTING:
            lower_msg = message.strip().lower()
            if "换" in message or "不喜欢" in message or "reject" in lower_msg:
                # Track rejected plan titles
                for plan in session.get("current_plans", []):
                    if isinstance(plan, PlanCard):
                        session["rejected_plans"].append(plan.title)
                    elif isinstance(plan, dict):
                        session["rejected_plans"].append(plan.get("title", ""))
                session["rejection_count"] = session.get("rejection_count", 0) + 1
                await analytics.track("plan_rejected", session_id=session_id, properties={
                    "rejection_count": session["rejection_count"],
                })

                # Parse rejection message for new preferences/constraints
                try:
                    parsed = await self.llm.parse_intent(message, history)
                    if parsed.get("constraints"):
                        for c in parsed["constraints"]:
                            if c not in ctx.constraints:
                                ctx.constraints.append(c)
                    if parsed.get("preferences"):
                        for p in parsed["preferences"]:
                            if p not in ctx.preferences:
                                ctx.preferences.append(p)
                    if parsed.get("energy_level") and parsed["energy_level"] != ctx.energy_level:
                        ctx.energy_level = parsed["energy_level"]
                    if parsed.get("companion_type") and not ctx.companion_type:
                        ctx.companion_type = parsed["companion_type"]
                    logger.info("Updated context from rejection: constraints=%s, preferences=%s",
                                ctx.constraints, ctx.preferences)
                except Exception:
                    logger.warning("Failed to parse rejection message for preferences")

                # Edge case: 3+ rejections → suggest refining preferences
                if session["rejection_count"] >= 3:
                    reply = ("看起来这些方案都不太合适 😅 "
                             "要不试试告诉我更具体的需求？比如想去什么类型的地方、预算范围、或者特别想做的事情？")
                    session["state"] = ConversationState.COLLECTING
                    history.append({"role": "assistant", "content": reply})
                    await self._save_session(session_id, session)
                    return ChatResponse(reply=reply, state=ConversationState.COLLECTING)

                session["state"] = ConversationState.GENERATING
                result = await self._generate_and_present(session, ctx, session_id)
                await self._save_session(session_id, session)
                return result

            if "选" in message or "select" in lower_msg:
                # Plan selected — acknowledge
                reply = "好的，方案已选择！祝你周末愉快 🎉"
                session["state"] = ConversationState.IDLE
                history.append({"role": "assistant", "content": reply})
                await self._save_session(session_id, session)
                return ChatResponse(reply=reply, state=ConversationState.IDLE)

            # Any other message → treat as new input, restart collecting
            session["state"] = ConversationState.COLLECTING

        # --- Parse intent via LLM ---
        parsed = await self.llm.parse_intent(message, history)
        await analytics.track("intent_parsed", session_id=session_id, properties={"parsed": parsed})

        # Merge parsed fields into context
        if parsed.get("city"):
            ctx.city = parsed["city"]
        if parsed.get("people_count"):
            ctx.people_count = parsed["people_count"]
        if parsed.get("companion_type"):
            ctx.companion_type = parsed["companion_type"]
        if parsed.get("energy_level"):
            ctx.energy_level = parsed["energy_level"]
        if parsed.get("constraints"):
            for c in parsed["constraints"]:
                if c not in ctx.constraints:
                    ctx.constraints.append(c)
        if parsed.get("preferences"):
            for p in parsed["preferences"]:
                if p not in ctx.preferences:
                    ctx.preferences.append(p)

        # --- Check if we have enough context ---
        if not ctx.city:
            session["state"] = ConversationState.COLLECTING
            reply = "你好！我是周末搭子 🎉 告诉我你想在哪个城市玩？和谁一起？有什么特殊需求吗？"
            history.append({"role": "assistant", "content": reply})
            await self._save_session(session_id, session)
            return ChatResponse(reply=reply, state=ConversationState.COLLECTING)

        # --- Enough context → generate plans ---
        session["state"] = ConversationState.GENERATING
        result = await self._generate_and_present(session, ctx, session_id)
        await self._save_session(session_id, session)
        return result

    async def _generate_and_present(
        self, session: dict[str, Any], ctx: UserContext, session_id: str = ""
    ) -> ChatResponse:
        """Parallel-fetch weather + POIs, then generate plans."""
        from app.services.crawler.config import CITIES as SUPPORTED_CITIES

        history: list[dict[str, str]] = session["history"]

        # Check if city is supported
        if ctx.city and ctx.city not in SUPPORTED_CITIES:
            supported = "、".join(SUPPORTED_CITIES)
            reply = f"抱歉，目前只支持 {supported} 的推荐 🙈 其他城市正在筹备中，敬请期待！"
            session["state"] = ConversationState.COLLECTING
            history.append({"role": "assistant", "content": reply})
            return ChatResponse(reply=reply, state=ConversationState.COLLECTING)

        try:
            plans = await asyncio.wait_for(
                self._do_generate(session, ctx, session_id),
                timeout=settings.llm_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Plan generation timed out after %ds for session %s", settings.llm_timeout, session_id)
            await analytics.track(
                "error",
                session_id=session_id,
                properties={"error_type": "timeout", "stage": "generate_plans"},
            )
            reply = "抱歉，生成方案花了太长时间，请稍后再试 🙏"
            session["state"] = ConversationState.COLLECTING
            history.append({"role": "assistant", "content": reply})
            return ChatResponse(reply=reply, state=ConversationState.COLLECTING)

        session["state"] = ConversationState.PRESENTING
        session["current_plans"] = plans

        await analytics.track("plans_generated", session_id=session_id, properties={"count": len(plans)})

        reply = "为您找到以下方案："
        history.append({"role": "assistant", "content": reply})

        return ChatResponse(
            reply=reply,
            plans=plans,
            state=ConversationState.PRESENTING,
        )

    async def _do_generate(
        self, session: dict[str, Any], ctx: UserContext, session_id: str = ""
    ) -> list[PlanCard]:
        """Inner generation logic: parallel weather + POI fetch, then plan generation."""
        weather_task = asyncio.create_task(self.weather.get_weather(ctx.city or "苏州"))
        pois_task = asyncio.create_task(
            self.data.get_pois(ctx.city or "苏州", ctx.preferences)
        )

        weather_data, pois_data = await asyncio.gather(weather_task, pois_task)

        logger.info("generate_plans input: city=%s, pois=%d, rejected=%s",
                     ctx.city, len(pois_data), session.get("rejected_plans"))
        if pois_data:
            poi_names = [p.get("name", "?") for p in pois_data[:5]]
            logger.info("POI names passed to LLM: %s", poi_names)

        context_dict = ctx.model_dump()

        plans = await self.plan_service.generate_plans(
            context=context_dict,
            weather=weather_data,
            pois=pois_data,
            rejected_plans=session.get("rejected_plans"),
        )

        return plans
