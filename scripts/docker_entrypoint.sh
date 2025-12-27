#!/bin/bash
# Docker entrypoint script for running the complete benchmark pipeline

set -e

echo "============================================================"
echo "SQL Query Optimization Benchmarking - Docker Pipeline"
echo "============================================================"
echo ""

# Wait for database to be ready
echo "Waiting for database to be ready..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" > /dev/null 2>&1; do
    echo "  Waiting for PostgreSQL..."
    sleep 2
done
echo "Database is ready!"
echo ""

# Run the complete benchmark suite
echo "Starting benchmark pipeline..."
python benchmarks/run_all_benchmarks.py

echo ""
echo "============================================================"
echo "Pipeline completed successfully!"
echo "============================================================"
echo ""
echo "Results available in:"
echo "  - Metrics: /app/results/metrics"
echo "  - Figures: /app/results/figures"
echo "  - Summary: /app/results/metrics/summary.csv"
