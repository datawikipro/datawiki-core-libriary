"""
Database configuration from environment variables.
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    
    # Connection settings
    url: Optional[str] = None
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = ""
    
    # Pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_recycle: int = 3600  # 1 hour
    pool_timeout: int = 30
    pool_pre_ping: bool = True
    
    # Retry settings
    max_retries: int = 5
    retry_delay: float = 2.0  # Base delay in seconds
    
    # Debug settings
    echo: bool = False
    echo_pool: bool = False
    
    @property
    def database_url(self) -> str:
        """Get the full database URL."""
        if self.url:
            return self.url
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @classmethod
    def from_env(cls, prefix: str = "") -> "DatabaseConfig":
        """
        Load configuration from environment variables.
        
        Args:
            prefix: Optional prefix for environment variables (e.g., "DB_")
        """
        load_dotenv()
        
        def get_env(key: str, default: str = "") -> str:
            return os.getenv(f"{prefix}{key}", os.getenv(key, default))
        
        def get_env_int(key: str, default: int) -> int:
            val = get_env(key, str(default))
            try:
                return int(val)
            except ValueError:
                return default
        
        def get_env_bool(key: str, default: bool) -> bool:
            val = get_env(key, str(default).lower())
            return val.lower() in ('true', '1', 'yes')
        
        return cls(
            url=get_env("DATABASE_URL") or None,
            host=get_env("POSTGRES_HOST", "localhost"),
            port=get_env_int("POSTGRES_PORT", 5432),
            database=get_env("POSTGRES_DB", "postgres"),
            user=get_env("POSTGRES_USER", "postgres"),
            password=get_env("POSTGRES_PASSWORD", ""),
            pool_size=get_env_int("DB_POOL_SIZE", 5),
            max_overflow=get_env_int("DB_MAX_OVERFLOW", 10),
            pool_recycle=get_env_int("DB_POOL_RECYCLE", 3600),
            pool_timeout=get_env_int("DB_POOL_TIMEOUT", 30),
            pool_pre_ping=get_env_bool("DB_POOL_PRE_PING", True),
            max_retries=get_env_int("DB_MAX_RETRIES", 5),
            retry_delay=float(get_env("DB_RETRY_DELAY", "2.0")),
            echo=get_env_bool("DB_ECHO", False),
            echo_pool=get_env_bool("DB_ECHO_POOL", False),
        )
