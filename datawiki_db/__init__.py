"""
Datawiki Database Core Library

Shared PostgreSQL database connection library for Datawiki microservices.
Provides robust connection management with retry logic, health checks, and metrics.
"""

from datawiki_db.engine import DatabaseManager
from datawiki_db.session import get_db, DatabaseSession
from datawiki_db.metrics import get_database_metrics, check_database_health
from datawiki_db.base import Base

__version__ = "1.0.0"
__all__ = [
    "DatabaseManager",
    "get_db",
    "DatabaseSession",
    "Base",
    "get_database_metrics",
    "check_database_health",
]
