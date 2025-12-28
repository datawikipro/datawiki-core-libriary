"""
Database session management for FastAPI and general use.
"""

import time
from typing import Generator, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

from datawiki_db.engine import get_db_manager
from datawiki_db.metrics import _metrics


def get_db() -> Generator[Session, None, None]:
    """
    Database session dependency for FastAPI.
    
    Provides a database session with:
    - Connection health check
    - Automatic retry on connection failures
    - Metrics tracking
    - Proper cleanup
    
    Usage:
        @app.get("/items")
        def get_items(db: Session = Depends(get_db)):
            return db.query(Item).all()
    """
    db_manager = get_db_manager()
    
    if db_manager.session_factory is None:
        raise Exception("Database connection not available")
    
    db: Optional[Session] = None
    session_start_time = time.time()
    _metrics.increment("total_sessions")
    
    max_retries = 3
    retry_delay = 1.0
    
    for attempt in range(max_retries):
        try:
            db = db_manager.create_session()
            
            # Test the connection
            db.execute(text("SELECT 1"))
            break  # Connection is good
            
        except Exception as e:
            _metrics.increment("connection_test_failures")
            if db:
                try:
                    db.close()
                except Exception:
                    pass
                db = None
            
            if attempt < max_retries - 1:
                _metrics.increment("reconnect_attempts")
                print(f"⚠️ Database connection failed (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay * (attempt + 1))
            else:
                _metrics.increment("failed_sessions")
                _metrics.record_failure(str(e))
                raise Exception(f"Database connection unavailable after {max_retries} attempts: {str(e)}")
    
    try:
        yield db
        _metrics.increment("successful_sessions")
    except GeneratorExit:
        _metrics.increment("generator_exit_count")
    except Exception as e:
        _metrics.increment("failed_sessions")
        _metrics.record_failure(str(e))
        if db:
            try:
                db.rollback()
            except Exception:
                pass
        raise
    finally:
        session_duration = time.time() - session_start_time
        if session_duration > 10.0:
            print(f"⚠️ Long-running database session: {session_duration:.2f}s")
        
        if db:
            try:
                db.close()
            except Exception:
                pass


class DatabaseSession:
    """
    Context manager for database sessions with retry logic.
    
    Usage:
        with DatabaseSession(auto_commit=True) as session:
            session.query(Model).all()
    """
    
    def __init__(self, auto_commit: bool = False, max_retries: int = 3):
        """
        Initialize database session context manager.
        
        Args:
            auto_commit: Whether to commit automatically on success
            max_retries: Maximum number of connection attempts
        """
        self.auto_commit = auto_commit
        self.max_retries = max_retries
        self.session: Optional[Session] = None
    
    def __enter__(self) -> Session:
        db_manager = get_db_manager()
        
        if db_manager.session_factory is None:
            raise Exception("Database connection not available")
        
        for attempt in range(self.max_retries):
            try:
                self.session = db_manager.create_session()
                # Test connection
                self.session.execute(text("SELECT 1"))
                return self.session
            except Exception as e:
                if self.session:
                    try:
                        self.session.close()
                    except Exception:
                        pass
                    self.session = None
                
                if attempt < self.max_retries - 1:
                    time.sleep(1.0 * (attempt + 1))
                else:
                    raise Exception(
                        f"Database session unavailable after {self.max_retries} attempts: {str(e)}"
                    )
        
        # This should never be reached, but satisfies type checker
        raise Exception("Failed to create database session")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            try:
                if exc_type is None and self.auto_commit:
                    self.session.commit()
                elif exc_type is not None:
                    try:
                        self.session.rollback()
                    except Exception as rollback_e:
                        print(f"⚠️ Warning: Error during rollback: {rollback_e}")
            except Exception as e:
                print(f"⚠️ Warning: Error during session cleanup: {e}")
            finally:
                try:
                    self.session.close()
                except Exception as e:
                    print(f"⚠️ Warning: Error closing session: {e}")
                self.session = None
