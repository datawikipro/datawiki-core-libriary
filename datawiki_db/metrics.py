"""
Database metrics and health checks.
"""

import time
from typing import Dict, Any, Optional
import threading


class DatabaseMetrics:
    """Thread-safe database metrics collector."""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._metrics = {
            "total_sessions": 0,
            "successful_sessions": 0,
            "failed_sessions": 0,
            "connection_test_failures": 0,
            "reconnect_attempts": 0,
            "generator_exit_count": 0,
            "last_failure": None,
            "last_failure_time": None,
        }
    
    def increment(self, key: str, value: int = 1) -> None:
        """Increment a metric counter."""
        with self._lock:
            if key in self._metrics and isinstance(self._metrics[key], int):
                self._metrics[key] += value
    
    def set(self, key: str, value: Any) -> None:
        """Set a metric value."""
        with self._lock:
            self._metrics[key] = value
    
    def record_failure(self, error: str) -> None:
        """Record a failure with timestamp."""
        with self._lock:
            self._metrics["last_failure"] = error
            self._metrics["last_failure_time"] = time.time()
    
    def get(self) -> Dict[str, Any]:
        """Get a copy of all metrics."""
        with self._lock:
            return self._metrics.copy()
    
    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._metrics = {
                "total_sessions": 0,
                "successful_sessions": 0,
                "failed_sessions": 0,
                "connection_test_failures": 0,
                "reconnect_attempts": 0,
                "generator_exit_count": 0,
                "last_failure": None,
                "last_failure_time": None,
            }


# Global metrics instance
_metrics = DatabaseMetrics()


def get_database_metrics() -> Dict[str, Any]:
    """Get current database metrics."""
    return _metrics.get()


def reset_database_metrics() -> None:
    """Reset database metrics."""
    _metrics.reset()


def check_database_health(engine) -> bool:
    """
    Check if database connection is healthy.
    
    Args:
        engine: SQLAlchemy engine instance
        
    Returns:
        True if connection is healthy, False otherwise
    """
    if engine is None:
        return False
        
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
