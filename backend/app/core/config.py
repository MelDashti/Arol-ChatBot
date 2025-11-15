"""
Application configuration using Pydantic settings
Supports environment variables and .env files
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, validator
from typing import List, Optional
import os


class Settings(BaseSettings):
    """Application settings with validation"""

    # Application
    APP_NAME: str = "Arol AI Chatbot"
    VERSION: str = "2.0.0"
    DEBUG: bool = Field(default=False, env="DEBUG")
    ENVIRONMENT: str = Field(default="production", env="ENVIRONMENT")

    # API
    API_PREFIX: str = "/api"
    SECRET_KEY: str = Field(..., env="SECRET_KEY")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # CORS
    CORS_ORIGINS: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8000"],
        env="CORS_ORIGINS"
    )

    # Database
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://postgres:postgres@db:5432/arol_chatbot",
        env="DATABASE_URL"
    )

    # Redis Cache
    REDIS_URL: str = Field(
        default="redis://redis:6379/0",
        env="REDIS_URL"
    )
    CACHE_TTL: int = Field(default=3600, env="CACHE_TTL")  # 1 hour

    # Pinecone
    PINECONE_API_KEY: str = Field(..., env="PINECONE_API_KEY")
    PINECONE_INDEX: str = Field(default="arolchatbot", env="PINECONE_INDEX")
    PINECONE_ENVIRONMENT: str = Field(default="us-west1-gcp-free", env="PINECONE_ENVIRONMENT")

    # ML Models
    MODEL_NAME: str = Field(default="Meldashti/chatbot", env="MODEL_NAME")
    BASE_MODEL: str = Field(default="unsloth/Llama-3.2-3B", env="BASE_MODEL")
    EMBEDDING_MODEL: str = Field(default="thenlper/gte-large", env="EMBEDDING_MODEL")
    MAX_NEW_TOKENS: int = Field(default=150, env="MAX_NEW_TOKENS")
    TEMPERATURE: float = Field(default=0.7, env="TEMPERATURE")
    TOP_K_RETRIEVAL: int = Field(default=5, env="TOP_K_RETRIEVAL")

    # Rate Limiting
    RATE_LIMIT_PER_MINUTE: int = Field(default=60, env="RATE_LIMIT_PER_MINUTE")
    RATE_LIMIT_PER_HOUR: int = Field(default=1000, env="RATE_LIMIT_PER_HOUR")

    # Monitoring
    SENTRY_DSN: Optional[str] = Field(default=None, env="SENTRY_DSN")
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")

    # File Upload
    MAX_UPLOAD_SIZE: int = Field(default=10 * 1024 * 1024, env="MAX_UPLOAD_SIZE")  # 10MB
    ALLOWED_EXTENSIONS: List[str] = Field(
        default=[".pdf", ".txt", ".docx", ".doc"],
        env="ALLOWED_EXTENSIONS"
    )

    # AWS (for production deployment)
    AWS_REGION: str = Field(default="us-east-1", env="AWS_REGION")
    AWS_ACCESS_KEY_ID: Optional[str] = Field(default=None, env="AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: Optional[str] = Field(default=None, env="AWS_SECRET_ACCESS_KEY")
    S3_BUCKET: Optional[str] = Field(default=None, env="S3_BUCKET")

    # Feature Flags
    ENABLE_ANALYTICS: bool = Field(default=True, env="ENABLE_ANALYTICS")
    ENABLE_FILE_UPLOAD: bool = Field(default=True, env="ENABLE_FILE_UPLOAD")
    ENABLE_VOICE: bool = Field(default=False, env="ENABLE_VOICE")
    ENABLE_MULTI_LANGUAGE: bool = Field(default=False, env="ENABLE_MULTI_LANGUAGE")

    @validator("CORS_ORIGINS", pre=True)
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"
    )


# Create global settings instance
settings = Settings()
