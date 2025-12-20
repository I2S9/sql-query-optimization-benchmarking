#!/usr/bin/env python3
"""
Index management script for SQL Query Optimization Benchmarking.
Applies or drops indexes to toggle between "no index" and "with index" configurations.
"""

import os
import sys
import argparse
import psycopg2
from pathlib import Path

# Default database configuration (can be overridden by environment variables)
DEFAULT_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "benchmark_db"),
    "user": os.getenv("DB_USER", "benchmark"),
    "password": os.getenv("DB_PASSWORD", "benchmark"),
}

# SQL directory
SQL_DIR = Path(__file__).parent.parent / "sql"


def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(**DEFAULT_CONFIG)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error connecting to database: {e}")
        print("\nMake sure PostgreSQL is running and check your environment variables:")
        print("  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
        sys.exit(1)


def execute_sql_file(conn, sql_file, action_name):
    """
    Execute SQL commands from a file.
    
    Args:
        conn: Database connection
        sql_file: Path to SQL file
        action_name: Description of the action (for logging)
    """
    if not sql_file.exists():
        print(f"Error: SQL file not found: {sql_file}")
        return False
    
    try:
        cur = conn.cursor()
        
        print(f"{action_name}...")
        
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_content = f.read()
        
        # Execute SQL commands
        cur.execute(sql_content)
        conn.commit()
        cur.close()
        
        print(f"{action_name} completed successfully")
        return True
        
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error {action_name.lower()}: {e}")
        return False
    except Exception as e:
        conn.rollback()
        print(f"Unexpected error: {e}")
        return False


def list_indexes(conn):
    """List all indexes in the database."""
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                schemaname,
                tablename,
                indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname;
        """)
        
        indexes = cur.fetchall()
        cur.close()
        
        return indexes
        
    except Exception as e:
        print(f"Error listing indexes: {e}")
        return []


def apply_indexes(conn):
    """Apply indexes from sql/indexes.sql."""
    indexes_file = SQL_DIR / "indexes.sql"
    return execute_sql_file(conn, indexes_file, "Applying indexes")


def drop_indexes(conn):
    """Drop all indexes using sql/drop_indexes.sql."""
    drop_file = SQL_DIR / "drop_indexes.sql"
    return execute_sql_file(conn, drop_file, "Dropping indexes")


def show_status(conn):
    """Show current index status."""
    indexes = list_indexes(conn)
    
    if not indexes:
        print("No indexes found in the database (baseline 'no index' configuration)")
    else:
        print(f"Found {len(indexes)} indexes:")
        current_table = None
        for schema, table, index in indexes:
            if table != current_table:
                if current_table is not None:
                    print()
                print(f"  {table}:")
                current_table = table
            print(f"    - {index}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Manage database indexes for benchmarking"
    )
    parser.add_argument(
        "action",
        choices=["apply", "drop", "status"],
        help="Action to perform: apply indexes, drop indexes, or show status"
    )
    
    args = parser.parse_args()
    
    # Connect to database
    conn = get_db_connection()
    
    try:
        if args.action == "apply":
            success = apply_indexes(conn)
            if success:
                print()
                show_status(conn)
            sys.exit(0 if success else 1)
            
        elif args.action == "drop":
            success = drop_indexes(conn)
            if success:
                print()
                show_status(conn)
            sys.exit(0 if success else 1)
            
        elif args.action == "status":
            show_status(conn)
            sys.exit(0)
            
    finally:
        conn.close()


if __name__ == "__main__":
    main()

