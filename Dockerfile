FROM python:3.11-slim

# Avoid .pyc files and enable unbuffered stdout/stderr for container logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python dependencies first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application scripts
COPY *.py ./

# Run as a non-root user
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# Default command; docker-compose overrides per service
CMD ["python", "-u", "ekco_gps_new_server.py"]
