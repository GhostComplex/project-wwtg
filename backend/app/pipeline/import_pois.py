"""Import POI data from JSON file into the database and Redis cache.

Usage:
    docker-compose exec api python -m app.pipeline.import_pois data/pois_上海.json
    docker-compose exec api python -m app.pipeline.import_pois data/pois.json --city 上海
"""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import POI data from JSON")
    parser.add_argument("file", type=str, help="Path to JSON file with POI data")
    parser.add_argument("--city", type=str, help="Override city for all POIs")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    from app.config import settings
    from app.models.schemas import POIData
    from app.services.data_service import DataService

    filepath = Path(args.file)
    if not filepath.exists():
        logger.error("File not found: %s", filepath)
        sys.exit(1)

    raw = json.loads(filepath.read_text(encoding="utf-8"))

    # Normalize input format
    if isinstance(raw, dict):
        if "results" in raw:
            # XHS search result format: {"count": N, "results": [...]}
            items_list = raw["results"]
            grouped = {args.city or "unknown": items_list}
        else:
            # Grouped format: {"上海": [...], "苏州": [...]}
            grouped = raw
    elif isinstance(raw, list):
        grouped = {args.city or "unknown": raw}
    else:
        logger.error("Invalid JSON format")
        sys.exit(1)

    # Resolve city
    for city_key in list(grouped.keys()):
        if city_key == "unknown":
            if not args.city:
                stem = filepath.stem
                for c in ["上海", "苏州", "杭州", "南京", "北京"]:
                    if c in stem:
                        args.city = c
                        break
                if not args.city:
                    logger.error("Cannot determine city. Use --city or name file like pois_上海.json")
                    sys.exit(1)
            grouped[args.city] = grouped.pop(city_key)

    # Redis
    redis_client = None
    try:
        import redis.asyncio as aioredis
        redis_client = aioredis.from_url(settings.redis_url)
        await redis_client.ping()
        logger.info("Connected to Redis")
    except Exception:
        logger.warning("Redis not available, skipping cache")

    # DB
    db_session = None
    try:
        from app.database import async_session
        db_session = async_session()
        logger.info("Connected to database")
    except Exception:
        logger.warning("Database not available, skipping DB persist")

    service = DataService(redis_client=redis_client, db_session=db_session)

    total = 0
    for city, items in grouped.items():
        pois = []
        for item in items:
            try:
                # Handle XHS search result format + POIData format
                note_id = item.get("id", item.get("note_id", ""))
                source_url = item.get("source_url", item.get("url"))
                if not source_url and note_id:
                    source_url = f"https://www.xiaohongshu.com/explore/{note_id}"

                likes = item.get("source_likes") or item.get("likes") or item.get("liked_count")
                if likes is not None:
                    likes = int(likes)

                poi = POIData(
                    name=item.get("name", item.get("title", "Unknown")),
                    address=item.get("address"),
                    city=args.city or city,
                    tags=item.get("tags", []),
                    description=item.get("description", item.get("content", "")),
                    cost_range=item.get("cost_range"),
                    suitable_for=item.get("suitable_for", []),
                    source_type="xiaohongshu",
                    source_url=source_url,
                    source_likes=likes,
                )
                pois.append(poi)
            except Exception as e:
                logger.warning("Skipping invalid item: %s", e)

        if pois:
            await service.cache_pois(city, pois)
            logger.info("Imported %d POIs for %s", len(pois), city)
            total += len(pois)

    logger.info("=== Import Complete: %d total POIs ===", total)

    if db_session:
        await db_session.commit()
        await db_session.close()
    if redis_client:
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())
