FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements_analysis.txt .
RUN pip install --no-cache-dir -r requirements_analysis.txt

# Copy application code
COPY . .

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV POSTGRES_HOST=postgres

# Run the WebSocket bot
CMD ["python", "kalshi_ws.py"]
