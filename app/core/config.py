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
    openrouter_api_key: str | None = Field(default=None, alias="OPENROUTER_API_KEY")
    openrouter_model: str = Field(
        default="openai/gpt-4o-mini", alias="OPENROUTER_MODEL"
    )
    ollama_host: str = Field(default="http://localhost:11434", alias="OLLAMA_HOST")
    ollama_model: str = Field(default="llama3.1:8b", alias="OLLAMA_MODEL")
    ollama_temperature: float = Field(default=0.0, alias="OLLAMA_TEMPERATURE")
    aip_parser_docling_quality_threshold: float = Field(
        default=0.2, alias="AIP_PARSER_DOCLING_QUALITY_THRESHOLD"
    )
    aip_parser_ocr_enabled: bool = Field(default=True, alias="AIP_PARSER_OCR_ENABLED")
    aip_parser_ocr_mode: Literal["document", "page"] = Field(
        default="page", alias="AIP_PARSER_OCR_MODE"
    )
    aip_parser_timeout_seconds: int = Field(
        default=60, alias="AIP_PARSER_TIMEOUT_SECONDS"
    )
    aip_parser_max_pages: int = Field(default=80, alias="AIP_PARSER_MAX_PAGES")

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
