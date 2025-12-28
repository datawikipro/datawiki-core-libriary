"""
Database engine management with retry logic.
"""

import time
import threading
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from datawiki_db.config import DatabaseConfig
from datawiki_db.metrics import _metrics


class DatabaseManager:
    """
    Thread-safe database connection manager with retry logic.
    
    Provides robust connection management with:
    - Automatic retry on connection failures
    - Exponential backoff
    - Connection health checks
    - Connection pool management
    """
    
    _instance: Optional["DatabaseManager"] = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """Singleton pattern for global database manager."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        """
        Initialize database manager.
        
        Args:
            config: Database configuration. If None, loads from environment.
        """
        if self._initialized:
            return
            
        self._engine_lock = threading.Lock()
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        self._config = config or DatabaseConfig.from_env()
        self._initialized = True
        
        print("🔧 DatabaseManager initialized")
    
    @property
    def engine(self) -> Optional[Engine]:
        """Get the database engine, initializing if necessary."""
        self._init_engine()
        return self._engine
    
    @property
    def session_factory(self) -> Optional[sessionmaker]:
        """Get the session factory, initializing if necessary."""
        self._init_engine()
        return self._session_factory
    
    def _init_engine(self) -> None:
        """Initialize database engine with retry logic."""
        with self._engine_lock:
            # Check if existing engine is healthy
            if self._engine is not None:
                try:
                    with self._engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                    return  # Engine is healthy
                except Exception as e:
                    print(f"⚠️ Existing database engine is broken: {e}")
                    self._reset_engine()
            
            # Create new engine with retries
            config = self._config
            for attempt in range(config.max_retries):
                print(f"🔧 Creating SQLAlchemy engine (attempt {attempt + 1}/{config.max_retries})...")
                try:
                    self._engine = create_engine(
                        config.database_url,
                        pool_size=config.pool_size,
                        max_overflow=config.max_overflow,
                        pool_recycle=config.pool_recycle,
                        pool_timeout=config.pool_timeout,
                        pool_pre_ping=config.pool_pre_ping,
                        echo=config.echo,
                        echo_pool=config.echo_pool,
                    )
                    
                    # Test connection
                    with self._engine.connect() as conn:
                        result = conn.execute(text("SELECT version()"))
                        version = result.fetchone()[0]
                        print(f"✅ Database connection successful. PostgreSQL: {version}")
                    
                    self._session_factory = sessionmaker(
                        autocommit=False, 
                        autoflush=False, 
                        bind=self._engine
                    )
                    _metrics.increment("reconnect_attempts", attempt)
                    return
                    
                except Exception as e:
                    print(f"❌ Error creating database engine (attempt {attempt + 1}/{config.max_retries}): {e}")
                    self._cleanup_engine()
                    
                    if attempt < config.max_retries - 1:
                        wait_time = config.retry_delay * (2 ** attempt)
                        print(f"⏳ Waiting {wait_time:.1f}s before retry...")
                        time.sleep(wait_time)
                    else:
                        _metrics.record_failure(str(e))
                        print(f"❌ Failed to connect to database after {config.max_retries} attempts")
    
    def _reset_engine(self) -> None:
        """Reset database connection for reconnection attempt."""
        self._cleanup_engine()
        self._engine = None
        self._session_factory = None
    
    def _cleanup_engine(self) -> None:
        """Cleanup existing engine resources."""
        if self._engine is not None:
            try:
                self._engine.dispose()
            except Exception:
                pass
            self._engine = None
            self._session_factory = None
    
    def create_session(self) -> Session:
        """
        Create a new database session.
        
        Returns:
            SQLAlchemy Session instance
            
        Raises:
            Exception: If database connection is not available
        """
        self._init_engine()
        
        if self._session_factory is None:
            raise Exception("Database connection not available")
        
        return self._session_factory()
    
    def health_check(self) -> bool:
        """
        Check if database connection is healthy.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        if self._engine is None:
            return False
            
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
    
    def dispose(self) -> None:
        """Dispose of the database engine and all connections."""
        with self._engine_lock:
            self._cleanup_engine()


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def init_database(config: Optional[DatabaseConfig] = None) -> DatabaseManager:
    """
    Initialize the database with optional configuration.
    
    Args:
        config: Database configuration. If None, loads from environment.
        
    Returns:
        DatabaseManager instance
    """
    global _db_manager
    _db_manager = DatabaseManager(config)
    return _db_manager
