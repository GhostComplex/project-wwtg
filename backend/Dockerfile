FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY app/ app/
COPY scripts/ scripts/
RUN chmod +x scripts/start.sh

EXPOSE 8000

CMD ["scripts/start.sh"]
