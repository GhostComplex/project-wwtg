"""Import XHS notes from JSON → LLM extract POIs → cache to DB/Redis.

Usage:
    docker-compose exec api python -m app.pipeline.import_notes /data/seed/shanghai.json --city 上海
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
    parser = argparse.ArgumentParser(description="Import XHS notes → extract POIs → cache")
    parser.add_argument("file", type=str, help="Path to JSON file with XHS note data")
    parser.add_argument("--city", type=str, required=True, help="City for these notes")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    from app.config import settings
    from app.models.schemas import CrawlResult
    from app.services.data_service import DataService
    from app.services.llm_service import LLMService

    filepath = Path(args.file)
    if not filepath.exists():
        logger.error("File not found: %s", filepath)
        sys.exit(1)

    raw = json.loads(filepath.read_text(encoding="utf-8"))

    # Extract items from {"count": N, "results": [...]} or plain [...]
    if isinstance(raw, dict) and "results" in raw:
        items = raw["results"]
    elif isinstance(raw, list):
        items = raw
    else:
        logger.error("Unsupported format. Expected {results: [...]} or [...]")
        sys.exit(1)

    # Convert to CrawlResult
    notes: list[CrawlResult] = []
    for item in items:
        note_id = item.get("id", item.get("note_id", ""))
        notes.append(CrawlResult(
            note_id=note_id,
            title=item.get("title", ""),
            content=item.get("content", item.get("desc", "")),
            likes=int(item.get("liked_count", item.get("likes", 0))),
            comments=int(item.get("comment_count", item.get("comments", 0))),
            shares=int(item.get("share_count", item.get("shares", 0))),
            author=item.get("user", item.get("author")),
            images=[item["cover_url"]] if item.get("cover_url") else item.get("images", []),
            tags=item.get("tags", []),
            url=item.get("url", f"https://www.xiaohongshu.com/explore/{note_id}"),
        ))

    logger.info("Loaded %d notes from %s", len(notes), filepath.name)

    # Redis
    redis_client = None
    try:
        import redis.asyncio as aioredis
        redis_client = aioredis.from_url(settings.redis_url)
        await redis_client.ping()
        logger.info("Connected to Redis")
    except Exception:
        logger.warning("Redis not available")

    # DB
    db_session = None
    try:
        from app.core.deps import async_session_factory
        db_session = async_session_factory()
        logger.info("Connected to database")
    except Exception:
        logger.warning("Database not available")

    # LLM
    llm = LLMService(
        api_key=settings.llm_api_key,
        base_url=settings.llm_base_url,
        model=settings.llm_model,
    )
    if not llm.api_key:
        logger.warning("LLM_API_KEY not set — will use mock POI extraction")

    service = DataService(
        llm_service=llm,
        redis_client=redis_client,
        db_session=db_session,
    )

    # Process notes → extract POIs → cache
    logger.info("Extracting POIs from %d notes for %s...", len(notes), args.city)
    pois = await service.process_notes(notes, args.city)
    logger.info("Extracted %d POIs", len(pois))

    # Verify POIs via AMAP geocode
    if pois and settings.amap_api_key:
        import httpx

        logger.info("Verifying %d POIs via AMAP...", len(pois))
        verified_count = 0
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            for poi in pois:
                name = poi.get("name", "")
                if not name:
                    poi["verified"] = False
                    continue
                try:
                    resp = await client.get(
                        "https://restapi.amap.com/v3/place/text",
                        params={
                            "key": settings.amap_api_key,
                            "keywords": name,
                            "city": args.city,
                            "citylimit": "true",
                            "output": "JSON",
                        },
                    )
                    data = resp.json()
                    if data.get("status") == "1" and data.get("pois"):
                        poi["verified"] = True
                        amap_poi = data["pois"][0]
                        # Enrich with real address from AMAP
                        poi["address"] = amap_poi.get("address", poi.get("address", ""))
                        poi["amap_name"] = amap_poi.get("name", "")
                        verified_count += 1
                    else:
                        poi["verified"] = False
                except Exception as e:
                    logger.warning("AMAP verify failed for %s: %s", name, e)
                    poi["verified"] = False
                await asyncio.sleep(0.2)  # Rate limit

        logger.info("Verified %d/%d POIs via AMAP", verified_count, len(pois))
    elif pois:
        logger.warning("AMAP_API_KEY not set — skipping POI verification")
        for poi in pois:
            poi["verified"] = False

    if pois:
        await service.cache_pois(args.city, pois)
        logger.info("Cached %d POIs for %s", len(pois), args.city)

    if db_session:
        await db_session.commit()
        await db_session.close()
    if redis_client:
        await redis_client.close()

    logger.info("=== Import Complete: %d POIs for %s ===", len(pois), args.city)


if __name__ == "__main__":
    asyncio.run(main())
