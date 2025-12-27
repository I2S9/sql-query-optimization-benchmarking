# Dockerfile for SQL Query Optimization Benchmarking
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files
COPY pyproject.toml ./
COPY config.yaml ./

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Copy project files
COPY . .

# Create results directories
RUN mkdir -p results/metrics results/figures data/raw

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DB_HOST=db
ENV DB_PORT=5432
ENV DB_NAME=benchmark_db
ENV DB_USER=benchmark
ENV DB_PASSWORD=benchmark

# Copy entrypoint script
COPY scripts/docker_entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker_entrypoint.sh

# Use entrypoint script
ENTRYPOINT ["/usr/local/bin/docker_entrypoint.sh"]
