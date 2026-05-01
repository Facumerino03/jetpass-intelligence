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
    aip_parser_docling_ocr_languages: str = Field(
        default="es,en", alias="AIP_PARSER_DOCLING_OCR_LANGUAGES"
    )
    aip_parser_docling_do_ocr: bool = Field(
        default=False, alias="AIP_PARSER_DOCLING_DO_OCR"
    )
    aip_parser_docling_force_full_page_ocr: bool = Field(
        default=True, alias="AIP_PARSER_DOCLING_FORCE_FULL_PAGE_OCR"
    )
    aip_parser_tesseract_lang: str = Field(default="spa+eng", alias="AIP_PARSER_TESSERACT_LANG")
    aip_parser_tesseract_psm: int = Field(default=6, alias="AIP_PARSER_TESSERACT_PSM")
    aip_parser_docling_do_table_structure: bool = Field(
        default=False, alias="AIP_PARSER_DOCLING_DO_TABLE_STRUCTURE"
    )
    aip_parser_docling_table_mode: Literal["accurate", "fast"] = Field(
        default="fast", alias="AIP_PARSER_DOCLING_TABLE_MODE"
    )
    aip_parser_docling_table_cell_matching: bool = Field(
        default=False, alias="AIP_PARSER_DOCLING_TABLE_CELL_MATCHING"
    )

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
