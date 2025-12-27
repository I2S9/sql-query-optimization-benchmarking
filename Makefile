# SQL Query Optimization Benchmarking - Makefile
# Provides convenient commands for running benchmarks

.PHONY: help install setup generate load benchmark analyze plot clean all docker-build docker-run docker-clean docker-all

help:
	@echo "SQL Query Optimization Benchmarking - Available commands:"
	@echo ""
	@echo "Local execution:"
	@echo "  make install       - Install Python dependencies"
	@echo "  make setup         - Setup database (start PostgreSQL)"
	@echo "  make generate      - Generate data for all scales"
	@echo "  make load          - Load data into database"
	@echo "  make benchmark     - Run all benchmarks"
	@echo "  make analyze       - Analyze results and generate summary"
	@echo "  make plot          - Generate plots from results"
	@echo "  make clean         - Clean generated data and results"
	@echo "  make all           - Run complete benchmark suite"
	@echo ""
	@echo "Docker execution (recommended for reproducibility):"
	@echo "  make docker-build  - Build Docker image"
	@echo "  make docker-run    - Run complete pipeline in Docker"
	@echo "  make docker-clean  - Stop containers and remove volumes"
	@echo "  make docker-all    - Build and run complete pipeline"
	@echo ""

install:
	pip install -e .

setup:
	@echo "Starting PostgreSQL database..."
	docker-compose up -d db
	@echo "Waiting for database to be ready..."
	@sleep 5
	@echo "Database ready!"

generate:
	@echo "Generating data for all scales..."
	python data/raw/generate_data.py --scale small
	python data/raw/generate_data.py --scale medium
	python data/raw/generate_data.py --scale large

load:
	@echo "Loading data into database..."
	python benchmarks/load_data.py --scale small
	python benchmarks/load_data.py --scale medium
	python benchmarks/load_data.py --scale large

benchmark:
	@echo "Running complete benchmark suite..."
	python benchmarks/run_all_benchmarks.py

analyze:
	@echo "Analyzing results..."
	python benchmarks/analyze_results.py

plot:
	@echo "Generating plots..."
	python benchmarks/plot_results.py

clean:
	@echo "Cleaning generated files..."
	rm -f data/raw/*.csv
	rm -f results/metrics/*.json
	rm -f results/metrics/*.csv
	rm -f results/figures/*.png
	@echo "Clean complete!"

all: install setup generate load benchmark analyze plot
	@echo ""
	@echo "Complete benchmark suite finished!"
	@echo "Results available in results/ directory"

docker-build:
	@echo "Building Docker image..."
	docker-compose build

docker-run:
	@echo "Running complete benchmark pipeline in Docker..."
	docker-compose up benchmark
	@echo ""
	@echo "Pipeline completed! Results available in results/ directory"

docker-clean:
	@echo "Cleaning Docker containers and volumes..."
	docker-compose down -v
	@echo "Clean complete!"

docker-all: docker-build docker-run
	@echo ""
	@echo "Docker pipeline completed successfully!"
	@echo "Results available in results/ directory"

