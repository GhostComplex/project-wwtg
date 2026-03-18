"""Dependency injection: database session, Redis client."""

from typing import AsyncGenerator, Optional

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings

# --- SQLAlchemy async engine & session factory ---
engine = create_async_engine(settings.database_url, echo=settings.debug, pool_pre_ping=True)
async_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# --- Redis singleton ---
_redis_client: Optional[Redis] = None


def get_redis_client() -> Redis:
    """Get or create the Redis singleton."""
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    return _redis_client


async def close_redis() -> None:
    """Close Redis connection on shutdown."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async database session."""
    async with async_session_factory() as session:
        yield session


async def get_redis() -> Redis:
    """Get Redis client for dependency injection."""
    return get_redis_client()
