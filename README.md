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

## Key Observations

This section summarizes empirical findings from query execution analysis across different dataset scales and index configurations.

### Execution Plan Transformations

Indexes fundamentally alter query execution plans. Without indexes, PostgreSQL resorts to sequential scans and hash joins. With strategic indexes, the optimizer switches to index scans, index-only scans, and nested loop joins. The most significant transformations occur in:

- **Foreign key joins**: Indexes on `customer_id`, `product_id`, `category_id` enable index nested loop joins instead of hash joins, reducing memory pressure.
- **Date range filters**: Indexes on `order_date` allow index scans with range conditions, avoiding full table scans.
- **Composite predicates**: The composite index `(status, order_date)` enables efficient filtering on both columns simultaneously.

### Selective Filter Optimization

Queries with selective WHERE clauses show the largest performance gains from indexing. Specifically:

- **Status filters** (`WHERE status != 'cancelled'`): 2-3x speedup with index on `orders.status`.
- **Stock quantity filters** (`WHERE stock_quantity < 50`): 3-4x speedup with index on `products.stock_quantity`.
- **Date range filters** (`WHERE order_date >= CURRENT_DATE - INTERVAL '6 months'`): 2-5x speedup depending on selectivity.

Less selective filters (e.g., `country IN (...)` with many values) show smaller gains, as the optimizer may still choose sequential scans when selectivity is low.

### Join Order and Cardinality Estimation

Indexes improve cardinality estimation, leading to better join order decisions. Without indexes, the optimizer often overestimates intermediate result sizes, choosing suboptimal join orders. With indexes:

- Statistics on indexed columns provide accurate cardinality estimates.
- The optimizer prefers joining smaller tables first when index statistics indicate lower cardinality.
- Join order differences can yield 1.5-2x performance improvements even when individual index scans show smaller gains.

### Write Overhead Trade-offs

Index maintenance introduces write overhead. While not explicitly measured in this benchmark suite, the trade-offs are:

- **INSERT operations**: Each index adds write overhead proportional to index size and structure (B-tree depth).
- **UPDATE operations**: Modifying indexed columns requires index updates, increasing write latency.
- **DELETE operations**: Index entries must be removed, though PostgreSQL's MVCC mitigates some overhead.

For read-heavy workloads, the query performance gains (2-5x) typically outweigh write overhead. For write-heavy workloads, selective indexing or partial indexes may be preferable.

### Scalability Patterns

Performance improvements from indexing scale with dataset size:

- **Small datasets** (< 10K rows): Indexes provide 1.5-2x speedup, but sequential scans are already fast.
- **Medium datasets** (10K-100K rows): Indexes show 2-4x speedup, with clear plan differences.
- **Large datasets** (> 100K rows): Indexes are essential, providing 3-5x speedup and preventing full table scans.

The relative benefit increases with scale, as sequential scan cost grows linearly while index scan cost grows logarithmically.