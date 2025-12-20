#!/usr/bin/env python3
"""
Script to apply the database schema to PostgreSQL.
This script can be used to test that the schema.sql file is valid.
"""

import sys
import psycopg2
from pathlib import Path

# Database connection parameters
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "benchmark_db",
    "user": "benchmark",
    "password": "benchmark",
}


def apply_schema():
    """Apply the schema.sql file to the database."""
    schema_path = Path(__file__).parent.parent / "sql" / "schema.sql"
    
    if not schema_path.exists():
        print(f"Error: Schema file not found at {schema_path}")
        sys.exit(1)
    
    try:
        # Read the schema file
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        
        # Connect to database
        print("Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Execute schema
        print("Applying schema...")
        cursor.execute(schema_sql)
        
        # Verify tables were created
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print(f"\nSchema applied successfully!")
        print(f"Created {len(tables)} tables:")
        for table in tables:
            print(f"  - {table[0]}")
        
        cursor.close()
        conn.close()
        print("\nSchema validation complete.")
        
    except psycopg2.OperationalError as e:
        print(f"Error connecting to database: {e}")
        print("\nMake sure PostgreSQL is running:")
        print("  docker-compose up -d db")
        sys.exit(1)
    except psycopg2.Error as e:
        print(f"Error applying schema: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    apply_schema()

