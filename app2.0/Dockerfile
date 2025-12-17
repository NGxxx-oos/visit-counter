# Stage 1: Build stage
FROM python:3.11-slim AS builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install dependencies
WORKDIR /app
COPY requirements.txt .
# Install production dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Final stage
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install necessary system dependencies for psycopg2 (PostgreSQL client)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from the builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# НОВОЕ: Копируем исполняемые файлы (uvicorn, fastapi и т.д.)
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Copy application code
WORKDIR /app
COPY app/ /app/

# Expose the port the app runs on
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
