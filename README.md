# SQL Query Optimization Benchmarking

> This project studies SQL query execution behavior in a relational database system. The focus is on performance, scalability, and execution trade-offs under different conditions.

## Local Environment Setup

### Prerequisites

- Docker and Docker Compose
- Python 3.8 or higher
- Git

### Database Setup

This project uses PostgreSQL 16. The easiest way to run it locally is using Docker Compose.

#### Quick Start

1. Start the PostgreSQL database:
   ```bash
   docker-compose up -d db
   ```

   Or use the setup script:
   ```bash
   # On Linux/Mac or Git Bash
   bash scripts/setup_db.sh

   # On Windows PowerShell
   .\scripts\setup_db.ps1
   ```

2. Verify the database is running:
   ```bash
   docker-compose exec db psql -U benchmark -d benchmark_db -c "SELECT version();"
   ```

#### Connection Details

- **Host**: localhost
- **Port**: 5432
- **Database**: benchmark_db
- **User**: benchmark
- **Password**: benchmark

#### Stop the Database

```bash
docker-compose down
```

To remove all data (volumes):
```bash
docker-compose down -v
```

### Python Environment

1. Install dependencies:
   ```bash
   pip install -e .
   ```

2. Or install with development tools:
   ```bash
   pip install -e ".[dev]"
   ```

### Running Benchmarks

The project includes a complete benchmark suite that can be run in multiple ways:

#### Option 1: Using Make (recommended)
```bash
# Run complete benchmark suite
make all

# Or run individual steps
make generate    # Generate data
make load        # Load data into database
make benchmark   # Run all benchmarks
make analyze     # Analyze results
make plot        # Generate plots
```

#### Option 2: Using the benchmark runner script
```bash
# Run complete benchmark suite (reads config.yaml)
python benchmarks/run_all_benchmarks.py
```

#### Option 3: Manual execution
```bash
# Generate data
python data/raw/generate_data.py --scale small

# Load data
python benchmarks/load_data.py --scale small

# Run benchmarks
python benchmarks/run_benchmarks.py --scale small --index-config no_index
python benchmarks/apply_indexes.py apply
python benchmarks/run_benchmarks.py --scale small --index-config with_index

# Analyze and plot
python benchmarks/analyze_results.py
python benchmarks/plot_results.py
```

### Configuration

All benchmark parameters are centralized in `config.yaml`:
- Database connection settings
- Data generation parameters (with fixed seed=42 for reproducibility)
- Benchmark configuration (warmup runs, measurement runs, etc.)
- Analysis settings

See `config.yaml` for details and customization options.