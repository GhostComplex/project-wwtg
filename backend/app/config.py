"""Application settings via pydantic-settings."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """All configuration from environment variables."""

    # App
    app_name: str = "周末搭子 API"
    debug: bool = False

    # Database
    database_url: str = "postgresql+asyncpg://wwtg:wwtg@localhost:5432/wwtg"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # LLM
    llm_api_key: str = ""
    llm_base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    llm_model: str = "qwen-plus"
    llm_timeout: int = 90

    # WeChat
    wx_app_id: str = ""
    wx_app_secret: str = ""

    # Weather
    weather_api_key: str = ""

    # Map
    amap_api_key: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
