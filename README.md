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
