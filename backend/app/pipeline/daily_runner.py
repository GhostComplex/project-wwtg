"""Daily pipeline runner: fetch AMAP POIs → cache to Redis/PostgreSQL.

Replaces the previous XHS crawler pipeline. Architecture unchanged:
daily_runner pulls data → writes to cache → chat reads from cache.

Usage:
    python -m app.pipeline.daily_runner
    python -m app.pipeline.daily_runner --city 上海 --limit 2
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WWTG daily data pipeline (AMAP)")
    parser.add_argument("--city", type=str, help="Single city to fetch (default: all)")
    parser.add_argument(
        "--limit", type=int, help="Max type categories per city (default: all)"
    )
    return parser.parse_args()


async def main() -> None:
    """Run the daily data pipeline using AMAP POI API."""
    args = parse_args()

    from app.config import settings
    from app.models.schemas import POIData
    from app.pipeline.amap_config import AMAP_PAGES_PER_TYPE, AMAP_TYPE_CODES, CITIES
    from app.services.amap_poi_service import AmapPoiService
    from app.services.data_service import DataService

    logger.info("=== Daily Pipeline Starting (AMAP) ===")

    if not settings.amap_api_key:
        logger.warning(
            "⚠️  AMAP_API_KEY not set. Pipeline will use mock data. "
            "Set AMAP_API_KEY in .env to fetch real POIs."
        )

    redis_client = None
    amap_service = None

    # --- Redis ---
    try:
        import redis.asyncio as aioredis

        redis_client = aioredis.from_url(settings.redis_url)
        await redis_client.ping()
        logger.info("Connected to Redis at %s", settings.redis_url)
    except Exception:
        logger.warning("Redis not available — running without cache persistence")
        redis_client = None

    # --- AMAP service ---
    amap_service = AmapPoiService(api_key=settings.amap_api_key)

    # --- Determine target cities and type codes ---
    target_cities = [args.city] if args.city else CITIES
    type_codes = AMAP_TYPE_CODES
    if args.limit:
        # Limit number of type categories
        limited = dict(list(type_codes.items())[: args.limit])
        type_codes = limited

    # --- Run pipeline ---
    try:
        service = DataService(redis_client=redis_client)

        for city in target_cities:
            logger.info("Fetching POIs for city: %s", city)
            raw_pois = await amap_service.fetch_city_pois(
                city=city,
                type_codes=type_codes,
                pages=AMAP_PAGES_PER_TYPE,
            )

            # Convert AMAP results to POIData
            poi_models: list[POIData] = []
            for raw in raw_pois:
                poi = POIData(
                    name=raw["name"],
                    address=raw.get("address"),
                    city=city,
                    tags=raw.get("tags", []),
                    source_type="amap",
                    rating=raw.get("rating"),
                    phone=raw.get("phone"),
                    location=raw.get("location"),
                    verified=True,  # AMAP POIs are verified real places
                )
                poi_models.append(poi)

            # Cache to Redis/PG
            await service.cache_pois(city, poi_models)
            logger.info("City %s: %d POIs cached", city, len(poi_models))

        logger.info("=== Pipeline Complete ===")

    except Exception:
        logger.exception("Pipeline failed with unexpected error")
    finally:
        await amap_service.close()
        if redis_client:
            await redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())
