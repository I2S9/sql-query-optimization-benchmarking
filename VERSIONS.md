# Version Information

This document lists the versions of key dependencies used in this project for reproducibility.

## Python Version
- Python: 3.8 or higher

## Database
- PostgreSQL: 16 (via Docker)
- Image: `postgres:16`

## Python Dependencies

The project uses `pyproject.toml` for dependency management. Key dependencies:

- `psycopg2-binary>=2.9.0` - PostgreSQL adapter
- `sqlalchemy>=2.0.0` - SQL toolkit
- `pandas>=2.0.0` - Data analysis
- `matplotlib>=3.7.0` - Plotting
- `pyyaml>=6.0.0` - YAML configuration parsing

## Reproducibility

### Fixed Seeds
- Data generation seed: `42` (defined in `data/raw/generate_data.py`)

### Configuration
- All benchmark parameters are centralized in `config.yaml`
- Database connection parameters can be overridden via environment variables

### Environment Variables
- `DB_HOST` - Database host (default: localhost)
- `DB_PORT` - Database port (default: 5432)
- `DB_NAME` - Database name (default: benchmark_db)
- `DB_USER` - Database user (default: benchmark)
- `DB_PASSWORD` - Database password (default: benchmark)

## Installation

To ensure reproducibility, install dependencies from `pyproject.toml`:

```bash
pip install -e .
```

Or with development tools:

```bash
pip install -e ".[dev]"
```

## Docker

The project uses Docker Compose for PostgreSQL. Ensure Docker and Docker Compose are installed and up to date.

