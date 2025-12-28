# datawiki-db-core

Shared PostgreSQL database connection library for Datawiki microservices.

## Features

- ✅ Robust retry logic with exponential backoff
- ✅ Connection health checks
- ✅ Thread-safe singleton pattern
- ✅ Metrics tracking
- ✅ FastAPI dependency injection support
- ✅ Context manager for manual session control

## Installation

From GitHub:
```bash
pip install git+https://github.com/datawikipro/datawiki-db-core.git
```

Or for local development:
```bash
pip install -e /path/to/datawiki-core-library
```

## Usage

### FastAPI Dependency

```python
from fastapi import Depends
from sqlalchemy.orm import Session
from datawiki_db import get_db

@app.get("/items")
def get_items(db: Session = Depends(get_db)):
    return db.query(Item).all()
```

### Context Manager

```python
from datawiki_db import DatabaseSession

with DatabaseSession(auto_commit=True) as session:
    items = session.query(Item).all()
```

### Custom Configuration

```python
from datawiki_db import DatabaseManager
from datawiki_db.config import DatabaseConfig

config = DatabaseConfig(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="pass",
    pool_size=10,
    max_retries=5
)

db_manager = DatabaseManager(config)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | - | Full PostgreSQL URL |
| `POSTGRES_HOST` | localhost | Database host |
| `POSTGRES_PORT` | 5432 | Database port |
| `POSTGRES_DB` | postgres | Database name |
| `POSTGRES_USER` | postgres | Username |
| `POSTGRES_PASSWORD` | - | Password |
| `DB_POOL_SIZE` | 5 | Connection pool size |
| `DB_MAX_OVERFLOW` | 10 | Max overflow connections |
| `DB_POOL_RECYCLE` | 3600 | Connection recycle time (seconds) |
| `DB_MAX_RETRIES` | 5 | Max connection retries |
| `DB_RETRY_DELAY` | 2.0 | Base retry delay (seconds) |

## Metrics

```python
from datawiki_db import get_database_metrics

metrics = get_database_metrics()
# {
#   "total_sessions": 100,
#   "successful_sessions": 98,
#   "failed_sessions": 2,
#   "reconnect_attempts": 1,
#   ...
# }
```
