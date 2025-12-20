#!/bin/bash

# Setup script for PostgreSQL database
# This script starts the PostgreSQL container and waits for it to be ready

set -e

echo "Starting PostgreSQL container..."
docker-compose up -d db

echo "Waiting for PostgreSQL to be ready..."
timeout=30
counter=0
until docker-compose exec -T db pg_isready -U benchmark > /dev/null 2>&1; do
    if [ $counter -ge $timeout ]; then
        echo "Error: PostgreSQL did not become ready within ${timeout} seconds"
        exit 1
    fi
    echo "Waiting for database... ($counter/$timeout)"
    sleep 1
    counter=$((counter + 1))
done

echo "PostgreSQL is ready!"
echo ""
echo "Connection details:"
echo "  Host: localhost"
echo "  Port: 5432"
echo "  Database: benchmark_db"
echo "  User: benchmark"
echo "  Password: benchmark"
echo ""
echo "To connect using psql:"
echo "  docker-compose exec db psql -U benchmark -d benchmark_db"
echo ""
echo "To stop the database:"
echo "  docker-compose down"

