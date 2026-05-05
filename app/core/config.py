from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = Field(default="jetpass-intelligence", alias="APP_NAME")
    environment: Literal["development", "staging", "production", "test"] = Field(
        default="development", alias="ENVIRONMENT"
    )
    debug: bool = Field(default=False, alias="DEBUG")
    mongodb_url: str | None = Field(default=None, alias="MONGODB_URL")
    mongodb_db_name: str = Field(default="jetpass_aeronautical", alias="MONGODB_DB_NAME")
    redis_url: str | None = Field(default=None, alias="REDIS_URL")
    notam_cache_ttl_hours: int = Field(default=4, alias="NOTAM_CACHE_TTL_HOURS")
    notam_location_sync_enabled: bool = Field(
        default=True, alias="NOTAM_LOCATION_SYNC_ENABLED"
    )
    notam_location_sync_interval_hours: int = Field(
        default=12, alias="NOTAM_LOCATION_SYNC_INTERVAL_HOURS"
    )
    notam_location_sync_on_startup: bool = Field(
        default=True, alias="NOTAM_LOCATION_SYNC_ON_STARTUP"
    )
    notam_location_sync_headless: bool = Field(
        default=True, alias="NOTAM_LOCATION_SYNC_HEADLESS"
    )
    openrouter_api_key: str | None = Field(default=None, alias="OPENROUTER_API_KEY")
    openrouter_model: str = Field(
        default="openai/gpt-4o-mini", alias="OPENROUTER_MODEL"
    )
    llm_provider: Literal["ollama", "openrouter", "groq", "nvidia", "google_ai_studio", "google_vertex"] = Field(
        default="ollama", alias="LLM_PROVIDER"
    )
    llm_temperature: float = Field(default=0.0, alias="LLM_TEMPERATURE")
    groq_api_key: str | None = Field(default=None, alias="GROQ_API_KEY")
    groq_model: str = Field(default="llama-3.3-70b-versatile", alias="GROQ_MODEL")
    nvidia_api_key: str | None = Field(default=None, alias="NVIDIA_API_KEY")
    nvidia_model: str = Field(default="meta/llama-3.3-70b-instruct", alias="NVIDIA_MODEL")
    google_ai_studio_api_key: str | None = Field(default=None, alias="GOOGLE_AI_STUDIO_API_KEY")
    google_ai_studio_model: str = Field(
        default="gemini-2.0-flash", alias="GOOGLE_AI_STUDIO_MODEL"
    )
    google_vertex_project_id: str | None = Field(default=None, alias="GOOGLE_VERTEX_PROJECT_ID")
    google_vertex_location: str = Field(default="global", alias="GOOGLE_VERTEX_LOCATION")
    google_vertex_model: str = Field(
        default="google/gemini-2.0-flash-001", alias="GOOGLE_VERTEX_MODEL"
    )
    google_vertex_credentials_file: str | None = Field(
        default=None, alias="GOOGLE_VERTEX_CREDENTIALS_FILE"
    )
    ollama_host: str = Field(default="http://localhost:11434", alias="OLLAMA_HOST")
    ollama_model: str = Field(default="llama3.1:8b", alias="OLLAMA_MODEL")
    ollama_temperature: float = Field(default=0.0, alias="OLLAMA_TEMPERATURE")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
