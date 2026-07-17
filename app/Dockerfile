# ─────────────────────────────────────────────────────────────
# Dockerfile — FastAPI Leaderboard API
# Build:  docker build -t leaderboard-api .
# Run:    docker run -p 8000:8000 -e REDIS_HOST=redis leaderboard-api
# ─────────────────────────────────────────────────────────────

FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install Python dependencies first (leverages Docker layer cache)
# If only main.py changes, this layer is not re-built
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Expose the API port
EXPOSE 8000

# Start the server on all interfaces so Docker port-mapping works
CMD sh -c "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"
