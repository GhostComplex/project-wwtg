FROM python:3.11-slim AS builder

WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir .

FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Install Playwright + Chromium browser with system deps
RUN playwright install --with-deps chromium

COPY app/ app/
COPY scripts/ scripts/
RUN chmod +x scripts/start.sh

EXPOSE 8000

CMD ["scripts/start.sh"]
