"""FastAPI application entry point."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.config import settings
from sqlalchemy import text

from app.core.deps import close_redis, engine, get_redis_client
from app.middleware import ErrorHandlingMiddleware, RequestLoggingMiddleware
from app.models.db import Base


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: startup and shutdown hooks."""
    print(f"🚀 Starting {settings.app_name}")
    # Log critical config for easier debugging
    print(f"📋 Config: LLM_BASE_URL={settings.llm_base_url}")
    print(f"📋 Config: LLM_MODEL={settings.llm_model}")
    print(f"📋 Config: LLM_TIMEOUT={settings.llm_timeout}s")
    print(f"📋 Config: LLM_API_KEY={'✅ set' if settings.llm_api_key else '❌ NOT SET (will use mock)'}")
    print(f"📋 Config: AMAP_API_KEY={'✅ set' if settings.amap_api_key else '❌ NOT SET'}")
    # Create all tables (idempotent — safe to call every startup)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Database tables ensured")
    yield
    # Shutdown: cleanup
    await close_redis()
    await engine.dispose()
    print(f"👋 Shutting down {settings.app_name}")


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)

# Middleware (order matters: first added = outermost)
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(ErrorHandlingMiddleware)

# CORS — allow mini-program and dev origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(api_router, prefix="/api/v1")


@app.get("/health")
async def health() -> dict:
    """Health check endpoint — verifies DB and Redis connectivity."""
    status: dict = {"status": "ok", "db": "ok", "redis": "ok"}

    # Check DB
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:
        status["db"] = f"error: {exc}"
        status["status"] = "degraded"

    # Check Redis
    try:
        redis = get_redis_client()
        await redis.ping()
    except Exception as exc:
        status["redis"] = f"error: {exc}"
        status["status"] = "degraded"

    return status
