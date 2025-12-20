#!/usr/bin/env python3
"""
EXPLAIN plan capture script for SQL Query Optimization Benchmarking.
Executes EXPLAIN ANALYZE BUFFERS FORMAT JSON for each query and saves the results.
"""

import os
import sys
import json
import re
import argparse
from datetime import datetime
from pathlib import Path
import psycopg2

# Default database configuration (can be overridden by environment variables)
DEFAULT_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "benchmark_db"),
    "user": os.getenv("DB_USER", "benchmark"),
    "password": os.getenv("DB_PASSWORD", "benchmark"),
}

# Directories
SQL_DIR = Path(__file__).parent.parent / "sql"
RESULTS_DIR = Path(__file__).parent.parent / "results" / "metrics" / "plans"


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


def parse_queries(queries_file):
    """
    Parse SQL queries from the queries.sql file.
    Returns a list of tuples: (query_number, description, sql_query)
    """
    if not queries_file.exists():
        print(f"Error: Queries file not found: {queries_file}")
        sys.exit(1)
    
    with open(queries_file, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Pattern to match query blocks: "-- Query N: Description" followed by SQL
    pattern = r'-- Query (\d+):\s*([^\n]+)\n(.*?)(?=\n-- Query \d+:|$)'
    matches = re.finditer(pattern, content, re.DOTALL)
    
    queries = []
    for match in matches:
        query_num = int(match.group(1))
        description = match.group(2).strip()
        sql = match.group(3).strip()
        
        # Clean up SQL: remove leading/trailing whitespace and comments
        sql_lines = []
        for line in sql.split('\n'):
            line = line.strip()
            # Skip empty lines and comment-only lines
            if line and not line.startswith('--'):
                sql_lines.append(line)
        
        sql_query = ' '.join(sql_lines)
        # Remove trailing semicolon if present
        if sql_query.endswith(';'):
            sql_query = sql_query[:-1]
        
        if sql_query:
            queries.append((query_num, description, sql_query))
    
    return queries


def execute_explain(conn, query, query_num, description):
    """
    Execute EXPLAIN ANALYZE BUFFERS FORMAT JSON for a query.
    Returns the JSON plan or None if error.
    """
    explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
    
    try:
        cur = conn.cursor()
        cur.execute(explain_query)
        result = cur.fetchone()
        cur.close()
        
        if result and result[0]:
            # result[0] is a list containing the plan JSON
            plan = result[0][0] if isinstance(result[0], list) else result[0]
            return plan
        else:
            print(f"  Warning: No plan returned for Query {query_num}")
            return None
            
    except psycopg2.Error as e:
        print(f"  Error executing Query {query_num}: {e}")
        return None
    except Exception as e:
        print(f"  Unexpected error for Query {query_num}: {e}")
        return None


def save_plan(plan_data, output_dir, query_num, description, index_config):
    """
    Save execution plan to JSON file.
    
    Args:
        plan_data: The JSON plan data
        output_dir: Directory to save the plan
        query_num: Query number
        description: Query description
        index_config: Index configuration name (e.g., "no_index", "with_index")
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create filename: query_01_no_index.json
    filename = f"query_{query_num:02d}_{index_config}.json"
    filepath = output_dir / filename
    
    # Prepare metadata
    output_data = {
        "metadata": {
            "query_number": query_num,
            "description": description,
            "index_configuration": index_config,
            "captured_at": datetime.now().isoformat(),
            "database": DEFAULT_CONFIG["database"],
            "host": DEFAULT_CONFIG["host"],
        },
        "plan": plan_data
    }
    
    # Save to JSON file
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    return filepath


def capture_plans(index_config="no_index"):
    """
    Capture execution plans for all queries.
    
    Args:
        index_config: Configuration name (e.g., "no_index", "with_index")
    """
    queries_file = SQL_DIR / "queries.sql"
    output_dir = RESULTS_DIR / index_config
    
    print(f"Capturing execution plans ({index_config} configuration)...")
    print(f"Database: {DEFAULT_CONFIG['database']} @ {DEFAULT_CONFIG['host']}:{DEFAULT_CONFIG['port']}")
    print(f"Output directory: {output_dir}")
    print()
    
    # Parse queries
    queries = parse_queries(queries_file)
    print(f"Found {len(queries)} queries to analyze")
    print()
    
    # Connect to database
    conn = get_db_connection()
    
    try:
        success_count = 0
        error_count = 0
        
        for query_num, description, sql_query in queries:
            print(f"Query {query_num}: {description}")
            
            # Execute EXPLAIN
            plan = execute_explain(conn, sql_query, query_num, description)
            
            if plan:
                # Save plan
                filepath = save_plan(plan, output_dir, query_num, description, index_config)
                
                # Extract execution time if available
                exec_time = None
                if isinstance(plan, dict) and "Execution Time" in plan:
                    exec_time = plan["Execution Time"]
                elif isinstance(plan, dict) and "Plan" in plan:
                    plan_node = plan.get("Plan", {})
                    if "Execution Time" in plan_node:
                        exec_time = plan_node["Execution Time"]
                
                if exec_time:
                    print(f"  Execution time: {exec_time:.2f} ms")
                print(f"  Saved to: {filepath}")
                success_count += 1
            else:
                error_count += 1
            
            print()
        
        # Summary
        print("=" * 60)
        print(f"Summary:")
        print(f"  Successful: {success_count}")
        print(f"  Errors: {error_count}")
        print(f"  Total: {len(queries)}")
        print(f"  Plans saved to: {output_dir}")
        
        return success_count == len(queries)
        
    finally:
        conn.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Capture EXPLAIN ANALYZE plans for all queries"
    )
    parser.add_argument(
        "--index-config",
        type=str,
        default="no_index",
        help="Index configuration name (default: no_index). Use 'with_index' after applying indexes."
    )
    
    args = parser.parse_args()
    
    if capture_plans(index_config=args.index_config):
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()

