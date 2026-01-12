ARG PYTHON_VERSION=3.13
FROM python:${PYTHON_VERSION}-slim

WORKDIR /app

# Install common system dependencies
# gcc, libpq-dev, python3-dev: often needed for building python libs (e.g. asyncpg, psycopg2)
# curl, git: utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    python3-dev \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set PYTHONPATH to include /app so modules can be imported easily
ENV PYTHONPATH=/app

# Default to appuser, but can be overridden if root is needed for pip install
USER appuser
