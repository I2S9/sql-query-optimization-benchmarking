#!/usr/bin/env python3
"""
Data loader for SQL Query Optimization Benchmarking.
Loads CSV data into PostgreSQL using COPY for efficient ingestion.
"""

import os
import sys
import argparse
import psycopg2
from pathlib import Path
from psycopg2.extras import execute_values

# Default database configuration (can be overridden by environment variables)
DEFAULT_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "benchmark_db"),
    "user": os.getenv("DB_USER", "benchmark"),
    "password": os.getenv("DB_PASSWORD", "benchmark"),
}

# Data directory
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"


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


def load_table_copy(conn, table_name, csv_file, columns):
    """
    Load data from CSV file into table using COPY FROM STDIN.
    
    Args:
        conn: Database connection
        table_name: Target table name
        csv_file: Path to CSV file
        columns: List of column names to load (excluding auto-generated IDs)
    """
    csv_path = DATA_DIR / csv_file
    
    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}")
        return False
    
    try:
        cur = conn.cursor()
        
        # Build COPY command - exclude auto-generated ID columns
        column_list = ", ".join(columns)
        copy_sql = f"COPY {table_name} ({column_list}) FROM STDIN WITH CSV HEADER"
        
        print(f"Loading {table_name} from {csv_file}...")
        
        with open(csv_path, "r", encoding="utf-8") as f:
            cur.copy_expert(copy_sql, f)
        
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        
        conn.commit()
        cur.close()
        
        print(f"  Loaded {count} records into {table_name}")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"  Error loading {table_name}: {e}")
        return False


def load_categories(conn):
    """Load categories table."""
    return load_table_copy(
        conn,
        "categories",
        "categories.csv",
        ["name", "description", "created_at"]
    )


def load_products(conn):
    """Load products table."""
    return load_table_copy(
        conn,
        "products",
        "products.csv",
        ["category_id", "name", "description", "price", "stock_quantity", "created_at"]
    )


def load_customers(conn):
    """Load customers table."""
    return load_table_copy(
        conn,
        "customers",
        "customers.csv",
        ["email", "first_name", "last_name", "country", "city", "created_at"]
    )


def load_orders(conn):
    """Load orders table."""
    return load_table_copy(
        conn,
        "orders",
        "orders.csv",
        ["customer_id", "order_date", "total_amount", "status", "shipping_country"]
    )


def load_order_items(conn):
    """Load order_items table."""
    return load_table_copy(
        conn,
        "order_items",
        "order_items.csv",
        ["order_id", "product_id", "quantity", "unit_price", "subtotal"]
    )


def apply_schema(conn):
    """Apply the database schema."""
    schema_path = Path(__file__).parent.parent / "sql" / "schema.sql"
    
    if not schema_path.exists():
        print(f"Error: Schema file not found: {schema_path}")
        return False
    
    try:
        cur = conn.cursor()
        
        print("Applying database schema...")
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        
        cur.execute(schema_sql)
        conn.commit()
        cur.close()
        
        print("Schema applied successfully")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"Error applying schema: {e}")
        return False


def load_data(scale="small"):
    """
    Load data into PostgreSQL database.
    
    Args:
        scale: Dataset scale ('small', 'medium', 'large')
    """
    print(f"Loading {scale} dataset into PostgreSQL...")
    print(f"Database: {DEFAULT_CONFIG['database']} @ {DEFAULT_CONFIG['host']}:{DEFAULT_CONFIG['port']}")
    print()
    
    # Connect to database
    conn = get_db_connection()
    
    try:
        # Apply schema first
        if not apply_schema(conn):
            return False
        
        print()
        
        # Load data in dependency order
        success = True
        
        if not load_categories(conn):
            success = False
        
        if not load_products(conn):
            success = False
        
        if not load_customers(conn):
            success = False
        
        if not load_orders(conn):
            success = False
        
        if not load_order_items(conn):
            success = False
        
        if success:
            print()
            print("Data loading complete!")
            
            # Display summary
            cur = conn.cursor()
            tables = ["categories", "products", "customers", "orders", "order_items"]
            print("\nSummary:")
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"  {table}: {count} records")
            cur.close()
        else:
            print("\nData loading completed with errors")
            return False
        
        return True
        
    finally:
        conn.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Load CSV data into PostgreSQL database"
    )
    parser.add_argument(
        "--scale",
        type=str,
        default="small",
        choices=["small", "medium", "large"],
        help="Dataset scale to load (default: small)"
    )
    
    args = parser.parse_args()
    
    if load_data(scale=args.scale):
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()

