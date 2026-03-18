#!/bin/bash
# Startup script for the backend service
set -e

echo "Starting WWTG Backend..."

# Create logs directory
mkdir -p logs

# Start server (table creation handled by FastAPI lifespan)
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
