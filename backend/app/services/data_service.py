"""Data pipeline service: caching and persistence for POI data.

Manages Redis cache and PostgreSQL storage for POIs.
Data source was XHS crawler, now AMAP POI API (daily_runner handles fetching).
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import select

from app.models.db import PoiCache
from app.models.schemas import POIData
from app.services.llm_service import LLMService

logger = logging.getLogger(__name__)

# Redis key patterns
_CACHE_KEY = "wwtg:pois:{city}"
_CACHE_TTL = 48 * 3600  # 48 hours


class DataService:
    """Manages POI data: caching and persistence.

    Args:
        redis_client: Async Redis client (or None for no-cache mode).
        db_session: Async SQLAlchemy session (or None for no-persist mode).
        llm_service: LLMService instance (or None for no LLM fallback).
    """

    def __init__(
        self,
        redis_client: Any = None,
        db_session: Any = None,
        llm_service: Optional[LLMService] = None,
        # Deprecated: crawler param kept for backward compat, ignored
        crawler: Any = None,
    ) -> None:
        self._redis = redis_client
        self._db = db_session
        self._llm = llm_service
        if crawler is not None:
            logger.warning(
                "DataService 'crawler' param is deprecated (AMAP migration). "
                "Crawler is no longer used."
            )

    # ------------------------------------------------------------------
    # Public API (preserved from W1 for backward compat)
    # ------------------------------------------------------------------

    async def get_pois(self, city: str, tags: list[str]) -> list[dict[str, Any]]:
        """Fetch POIs for a city/tag combo from cache, crawler, or LLM fallback."""
        pois = await self.get_cached_pois(city, tags)
        if pois:
            logger.info("Found %d cached POIs for %s (real data)", len(pois), city)
        else:
            logger.info("No cached POIs for %s, trying LLM fallback (ai_generated)", city)
            pois = await self.generate_fallback_pois(city, tags)
        return pois

    async def generate_fallback_pois(self, city: str, tags: list[str] | None = None) -> list[dict[str, Any]]:
        """Generate POIs using LLM general knowledge when no crawler data available.

        Returns POIs marked with source_type='ai_generated'.
        """
        if self._llm is None or not getattr(self._llm, "api_key", None):
            logger.warning("No LLM available for fallback POI generation")
            return []

        prompt = (
            f"为城市「{city}」推荐8-10个适合周末出游的地点（POI）。"
        )
        if tags:
            prompt += f"用户偏好：{', '.join(tags)}。"
        prompt += (
            "返回JSON数组，每个元素包含：name, address, tags(数组), description, "
            "cost_range, suitable_for(数组)。只返回JSON，不要其他文字。"
        )

        try:
            raw = await self._llm.chat_completion(
                "你是一个旅游推荐助手，熟悉中国各城市的热门和小众景点。",
                prompt,
                max_tokens=1500,
            )
            import json as _json
            result = _json.loads(raw)
            if isinstance(result, dict) and "pois" in result:
                result = result["pois"]
            if not isinstance(result, list):
                result = [result]

            pois: list[dict[str, Any]] = []
            for item in result:
                poi = {
                    "name": item.get("name", "未知地点"),
                    "address": item.get("address"),
                    "city": city,
                    "tags": item.get("tags", []),
                    "description": item.get("description"),
                    "cost_range": item.get("cost_range"),
                    "suitable_for": item.get("suitable_for", []),
                    "source_type": "ai_generated",
                    "source_url": None,
                    "source_likes": None,
                }
                pois.append(poi)
            logger.info("LLM fallback generated %d POIs for %s", len(pois), city)
            return pois
        except Exception:
            logger.exception("LLM fallback POI generation failed for %s", city)
            return []

    async def refresh_cache(self, city: str) -> int:
        """Trigger a cache refresh for a city. Returns number of POIs updated.

        Note: With AMAP migration, this now requires external POI fetching.
        Use daily_runner for full refresh. This method is kept for backward compat.
        """
        logger.warning(
            "refresh_cache called but crawler is deprecated. "
            "Use daily_runner with AMAP for full refresh."
        )
        return 0

    async def get_cache_stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        stats: dict[str, Any] = {"total_pois": 0, "cities_cached": [], "last_refresh": None}
        if self._redis is None:
            return stats
        for city in CITIES:
            key = _CACHE_KEY.format(city=city)
            try:
                raw = await self._redis.get(key)
                if raw:
                    pois = json.loads(raw)
                    stats["total_pois"] += len(pois)
                    stats["cities_cached"].append(city)
            except Exception:
                pass
        return stats

    # ------------------------------------------------------------------
    # Pipeline (deprecated — daily_runner now handles fetching via AMAP)
    # ------------------------------------------------------------------

    async def run_daily_pipeline(
        self,
        cities: list[str] | None = None,
        keyword_limit: int | None = None,
    ) -> dict[str, int]:
        """Deprecated: daily_runner now handles AMAP fetching directly.

        Kept for backward compatibility. Returns empty results.
        """
        logger.warning(
            "DataService.run_daily_pipeline is deprecated. "
            "Use daily_runner directly for AMAP-based pipeline."
        )
        return {}

    async def _crawl_city(self, city: str, keyword_limit: int | None = None) -> list[POIData]:
        """Deprecated: XHS crawling removed. Returns empty list."""
        logger.warning("_crawl_city is deprecated (AMAP migration)")
        return []

    # ------------------------------------------------------------------
    # Processing (deprecated — AMAP returns structured data directly)
    # ------------------------------------------------------------------

    async def process_notes(self, notes: list[Any], city: str) -> list[POIData]:
        """Deprecated: XHS note processing removed."""
        logger.warning("process_notes is deprecated (AMAP migration)")
        return []

    # ------------------------------------------------------------------
    # Caching
    # ------------------------------------------------------------------

    async def cache_pois(self, city: str, pois: list[POIData]) -> None:
        """Write POIs to Redis (48h TTL) and PostgreSQL (upsert).

        Args:
            city: City key.
            pois: List of POIData to cache.
        """
        payload = json.dumps(
            [p.model_dump() for p in pois],
            ensure_ascii=False,
        )

        # Redis
        if self._redis is not None:
            key = _CACHE_KEY.format(city=city)
            try:
                await self._redis.set(key, payload, ex=_CACHE_TTL)
                logger.debug("Cached %d POIs for %s in Redis (TTL=%ds)", len(pois), city, _CACHE_TTL)
            except Exception:
                logger.warning("Failed to cache POIs in Redis for %s", city)

        # PostgreSQL upsert
        if self._db is not None:
            try:
                await self._upsert_pois_to_db(city, pois)
                logger.debug("Persisted %d POIs for %s in PostgreSQL", len(pois), city)
            except Exception:
                logger.exception("Failed to persist POIs in PostgreSQL for %s", city)

    async def _upsert_pois_to_db(self, city: str, pois: list[POIData]) -> None:
        """Upsert POIs into poi_cache table. Match by source_url or name+city."""
        expires = datetime.now(timezone.utc) + timedelta(seconds=_CACHE_TTL)

        for poi in pois:
            # Try to find existing record
            stmt = select(PoiCache)
            if poi.source_url:
                stmt = stmt.where(PoiCache.source_url == poi.source_url)
            else:
                stmt = stmt.where(PoiCache.city == city, PoiCache.poi_data["name"].astext == poi.name)

            result = await self._db.execute(stmt)
            existing = result.scalars().first()

            poi_json = poi.model_dump()
            if existing:
                existing.poi_data = poi_json
                existing.tags = poi.tags
                existing.source_likes = poi.source_likes
                existing.fetched_at = datetime.now(timezone.utc)
                existing.expires_at = expires
            else:
                record = PoiCache(
                    city=city,
                    tags=poi.tags,
                    poi_data=poi_json,
                    source_url=poi.source_url,
                    source_likes=poi.source_likes,
                    expires_at=expires,
                )
                self._db.add(record)

        await self._db.commit()

    async def get_pois_from_db(
        self, city: str, tags: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Fetch POIs from PostgreSQL as fallback when Redis misses.

        Args:
            city: City to query.
            tags: Optional tag filter.

        Returns:
            List of POI dicts.
        """
        if self._db is None:
            return []

        try:
            now = datetime.now(timezone.utc)
            stmt = select(PoiCache).where(
                PoiCache.city == city,
                PoiCache.expires_at > now,
            )
            result = await self._db.execute(stmt)
            rows = result.scalars().all()

            pois = [row.poi_data for row in rows]
            if tags:
                tag_set = set(tags)
                pois = [p for p in pois if tag_set & set(p.get("tags", []))]

            logger.info("DB fallback: %d POIs for %s", len(pois), city)
            return pois
        except Exception:
            logger.exception("Failed to read POIs from DB for %s", city)
            return []

    async def get_cached_pois(self, city: str, tags: list[str] | None = None) -> list[dict[str, Any]]:
        """Read POIs from Redis cache, falling back to DB, optionally filtered by tags.

        Args:
            city: City to query.
            tags: Optional tag filter (any-match).

        Returns:
            List of POI dicts. Empty list on cache miss.
        """
        if self._redis is not None:
            key = _CACHE_KEY.format(city=city)
            try:
                raw = await self._redis.get(key)
                if raw:
                    pois: list[dict[str, Any]] = json.loads(raw)
                    logger.info("Cache HIT for %s: %d POIs", city, len(pois))
                    if tags:
                        tag_set = set(tags)
                        # Soft filter: POIs with matching tags first, then untagged/non-matching
                        matched = [p for p in pois if tag_set & set(p.get("tags", []))]
                        unmatched = [p for p in pois if not (tag_set & set(p.get("tags", [])))]
                        pois = matched + unmatched
                    return pois
                logger.info("Cache MISS for %s", city)
            except Exception:
                logger.warning("Failed to read cached POIs for %s", city)

        # Fallback to DB
        return await self.get_pois_from_db(city, tags)
