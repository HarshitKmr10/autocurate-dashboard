"""Application configuration settings."""

from pydantic_settings import BaseSettings
from typing import List, Optional
import os


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    # Database Configuration
    database_url: str = "postgresql://user:pass@localhost:5432/autocurate"
    supabase_url: Optional[str] = None
    supabase_anon_key: Optional[str] = None
    supabase_service_role_key: Optional[str] = None
    
    # Redis Configuration
    redis_url: str = "redis://localhost:6379/0"
    
    # OpenAI Configuration
    openai_api_key: str
    openai_model: str = "gpt-4o-mini"
    
    # Security
    secret_key: str = "your-super-secret-key-change-this-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    
    # CORS Configuration
    allowed_origins: List[str] = ["http://localhost:3000", "http://localhost:3001"]
    
    # File Upload Configuration
    max_file_size: int = 15 * 1024 * 1024  # 15MB
    upload_dir: str = "./data/uploads"
    
    # Application Configuration
    app_name: str = "Autocurate Analytics Dashboard"
    app_version: str = "1.0.0"
    debug: bool = True
    log_level: str = "INFO"
    
    # Analytics Configuration
    default_sample_size: int = 1000
    max_chart_points: int = 500
    cache_ttl_seconds: int = 300
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get application settings."""
    return settings