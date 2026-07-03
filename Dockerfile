# syntax=docker/dockerfile:1.7
# ---------------------------------------------------------------------------
# Lakehouse-Pattern base image
#
# Ships the exact Python + Java combo required by pyspark 3.5.1 / delta-spark
# 3.2.0. Used by every service in docker-compose.yml (app, mlflow, streamlit)
# so they share one build layer.
# ---------------------------------------------------------------------------
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:${PATH}"

# System deps: JDK 17 for PySpark, curl for healthchecks, git for pip installs
# from source, procps so healthchecks can `pgrep`.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        git \
        procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first for better layer caching.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy source last so code edits don't invalidate the dep layer.
COPY . .

# Default: idle shell — docker-compose overrides `command` per service.
CMD ["bash"]
