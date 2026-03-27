"""Daily pipeline runner: fetch AMAP POIs → LLM enrich → cache to Redis/PostgreSQL.

Replaces the previous XHS crawler pipeline. Architecture unchanged:
daily_runner pulls data → enriches with LLM → writes to cache → chat reads from cache.

Usage:
    python -m app.pipeline.daily_runner
    python -m app.pipeline.daily_runner --city 上海 --limit 2
    python -m app.pipeline.daily_runner --no-llm  # skip LLM enrichment

Docker usage (write to container Redis so API can read it):
    docker exec <api-container> python -m app.pipeline.daily_runner --city 苏州

Or from host, point to Docker-mapped Redis port:
    python -m app.pipeline.daily_runner --city 苏州 --redis-url redis://localhost:6380/0
"""

import argparse
import asyncio
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _get_season() -> str:
    """Return current season in Chinese based on month."""
    from datetime import datetime
    month = datetime.now().month
    if month in (3, 4, 5):
        return "春天"
    elif month in (6, 7, 8):
        return "夏天"
    elif month in (9, 10, 11):
        return "秋天"
    else:
        return "冬天"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WWTG daily data pipeline (AMAP)")
    parser.add_argument("--city", type=str, help="Single city to fetch (default: all)")
    parser.add_argument(
        "--limit", type=int, help="Max type categories per city (default: all)"
    )
    parser.add_argument(
        "--no-llm", action="store_true", help="Skip LLM enrichment (tags/reason)"
    )
    parser.add_argument(
        "--redis-url", type=str, default="",
        help="Override REDIS_URL (e.g. redis://localhost:6380/0 for Docker mapped port)",
    )
    return parser.parse_args()


async def main() -> None:
    """Run the daily data pipeline using AMAP POI API + LLM enrichment."""
    args = parse_args()

    from app.config import settings
    from app.models.schemas import POIData
    from app.pipeline.amap_config import AMAP_PAGES_PER_TYPE, AMAP_TYPE_CODES, CITIES
    from app.services.amap_poi_service import AmapPoiService
    from app.services.data_service import DataService
    from app.services.llm_service import LLMService

    logger.info("=== Daily Pipeline Starting (AMAP + LLM) ===")

    if not settings.amap_api_key:
        logger.warning(
            "⚠️  AMAP_API_KEY not set. Pipeline will use mock data. "
            "Set AMAP_API_KEY in .env to fetch real POIs."
        )

    redis_client = None
    amap_service = None
    llm_service = None

    # --- Redis ---
    redis_url = args.redis_url or settings.redis_url
    try:
        import redis.asyncio as aioredis

        redis_client = aioredis.from_url(redis_url)
        await redis_client.ping()
        logger.info("Connected to Redis at %s", redis_url)
    except Exception:
        logger.warning("Redis not available — running without cache persistence")
        redis_client = None

    # --- AMAP service ---
    amap_service = AmapPoiService(api_key=settings.amap_api_key)

    # --- LLM service (for enrichment) ---
    if not args.no_llm:
        llm_service = LLMService(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model=settings.llm_model,
        )
        if settings.llm_api_key:
            logger.info("LLM enrichment enabled (model: %s)", settings.llm_model)
        else:
            logger.warning("LLM_API_KEY not set — recommendations will use mock fallback")
    else:
        logger.info("LLM enrichment disabled (--no-llm)")

    # --- Determine target cities and type codes ---
    target_cities = [args.city] if args.city else CITIES
    type_codes = AMAP_TYPE_CODES
    if args.limit:
        limited = dict(list(type_codes.items())[: args.limit])
        type_codes = limited

    season = _get_season()

    # --- Run pipeline ---
    try:
        service = DataService(redis_client=redis_client)

        for city in target_cities:
            logger.info("Fetching POIs for city: %s", city)

            # Step 1: Fetch AMAP POIs
            raw_pois = await amap_service.fetch_city_pois(
                city=city,
                type_codes=type_codes,
                pages=AMAP_PAGES_PER_TYPE,
            )

            # Step 2: LLM enrichment (tags, reason, suitable_for, cost_range)
            recommendations: list[dict] = []
            if llm_service and raw_pois:
                logger.info("Generating LLM recommendations for %d POIs...", len(raw_pois))
                enrichment_input = [
                    {**p, "city": city} for p in raw_pois
                ]
                recommendations = await llm_service.generate_poi_recommendations(
                    enrichment_input, season=season
                )

            # Step 3: Merge AMAP data + LLM recommendations → POIData
            poi_models: list[POIData] = []
            for idx, raw in enumerate(raw_pois):
                rec = recommendations[idx] if idx < len(recommendations) else {}

                # Merge tags: AMAP type-based tags + LLM-generated tags
                amap_tags = raw.get("tags", [])
                llm_tags = rec.get("tags", [])
                merged_tags = list(dict.fromkeys(amap_tags + llm_tags))  # dedupe, preserve order

                poi = POIData(
                    name=raw["name"],
                    address=raw.get("address") or None,
                    city=city,
                    tags=merged_tags,
                    description=rec.get("reason", ""),
                    cost_range=rec.get("cost_range"),
                    suitable_for=rec.get("suitable_for", []),
                    source_type="amap",
                    rating=raw.get("rating"),
                    phone=raw.get("phone") or None,
                    location=raw.get("location") or None,
                    verified=True,
                )
                poi_models.append(poi)

            # Step 4: Cache to Redis/PG
            await service.cache_pois(city, poi_models)
            enriched_count = sum(1 for r in recommendations if r.get("reason"))
            logger.info(
                "City %s: %d POIs cached (%d with LLM recommendations)",
                city, len(poi_models), enriched_count,
            )

        logger.info("=== Pipeline Complete ===")

    except Exception:
        logger.exception("Pipeline failed with unexpected error")
    finally:
        await amap_service.close()
        if llm_service:
            await llm_service.close()
        if redis_client:
            await redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())
