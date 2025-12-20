#!/usr/bin/env python3
"""
Benchmark runner for SQL Query Optimization Benchmarking.
Measures query execution latency with multiple runs and statistical analysis.
"""

import os
import sys
import json
import csv
import re
import time
import argparse
import statistics
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
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
RESULTS_DIR = Path(__file__).parent.parent / "results" / "metrics"

# Benchmark configuration
DEFAULT_WARMUP_RUNS = 2
DEFAULT_MEASUREMENT_RUNS = 10
DEFAULT_CONCURRENCY = 4
DEFAULT_DURATION_SECONDS = 30


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


def execute_query_timing(conn, query: str) -> float:
    """
    Execute a query and return execution time in milliseconds.
    
    Args:
        conn: Database connection
        query: SQL query to execute
    
    Returns:
        Execution time in milliseconds
    """
    cur = conn.cursor()
    
    try:
        t0 = time.perf_counter()
        cur.execute(query)
        cur.fetchall()  # Fetch all results to ensure complete execution
        t1 = time.perf_counter()
        
        dt = (t1 - t0) * 1000  # Convert to milliseconds
        return dt
    finally:
        cur.close()


def run_benchmark(conn, query: str, query_num: int, warmup_runs: int, measurement_runs: int) -> Dict[str, Any]:
    """
    Run benchmark for a single query.
    
    Args:
        conn: Database connection
        query: SQL query to benchmark
        query_num: Query number
        warmup_runs: Number of warmup runs
        measurement_runs: Number of measurement runs
    
    Returns:
        Dictionary with benchmark results
    """
    print(f"  Running {warmup_runs} warmup runs...", end=" ", flush=True)
    
    # Warmup runs (discard results)
    for _ in range(warmup_runs):
        execute_query_timing(conn, query)
    
    print("Done")
    print(f"  Running {measurement_runs} measurement runs...", end=" ", flush=True)
    
    # Measurement runs
    timings = []
    for i in range(measurement_runs):
        timing = execute_query_timing(conn, query)
        timings.append(timing)
        if (i + 1) % 5 == 0:
            print(".", end="", flush=True)
    
    print(" Done")
    
    # Calculate statistics
    timings_sorted = sorted(timings)
    n = len(timings_sorted)
    
    # Calculate percentiles (using nearest-rank method)
    def percentile(sorted_list, p):
        """Calculate percentile using nearest-rank method."""
        if not sorted_list:
            return None
        index = int((p / 100.0) * len(sorted_list))
        # Ensure index is within bounds
        index = min(index, len(sorted_list) - 1)
        return sorted_list[index]
    
    stats = {
        "runs": measurement_runs,
        "raw_timings": timings,  # Store all raw timings
        "min": min(timings) if timings else None,
        "max": max(timings) if timings else None,
        "mean": statistics.mean(timings) if timings else None,
        "median": statistics.median(timings) if timings else None,
        "p50": statistics.median(timings) if timings else None,  # p50 is the median
        "p95": percentile(timings_sorted, 95),
        "p99": percentile(timings_sorted, 99),
        "stddev": statistics.stdev(timings) if len(timings) > 1 else 0.0,
    }
    
    return stats


def save_results_json(results: Dict[str, Any], output_file: Path):
    """Save benchmark results to JSON file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"  Results saved to: {output_file}")


def save_results_csv(results: Dict[str, Any], output_file: Path):
    """Save benchmark results summary to CSV file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            "query_number", "description", "runs", "min_ms", "max_ms", 
            "mean_ms", "median_ms", "p50_ms", "p95_ms", "p99_ms", "stddev_ms"
        ])
        
        # Data rows
        for query_result in results["queries"]:
            stats = query_result["statistics"]
            writer.writerow([
                query_result["query_number"],
                query_result["description"],
                stats["runs"],
                f"{stats['min']:.2f}",
                f"{stats['max']:.2f}",
                f"{stats['mean']:.2f}",
                f"{stats['median']:.2f}",
                f"{stats['p50']:.2f}",
                f"{stats['p95']:.2f}",
                f"{stats['p99']:.2f}",
                f"{stats['stddev']:.2f}",
            ])
    
    print(f"  Summary CSV saved to: {output_file}")


def run_benchmarks(scale: str = "small", index_config: str = "no_index", 
                   warmup_runs: int = DEFAULT_WARMUP_RUNS, 
                   measurement_runs: int = DEFAULT_MEASUREMENT_RUNS):
    """
    Run benchmarks for all queries.
    
    Args:
        scale: Dataset scale (small, medium, large)
        index_config: Index configuration (no_index, with_index)
        warmup_runs: Number of warmup runs per query
        measurement_runs: Number of measurement runs per query
    """
    queries_file = SQL_DIR / "queries.sql"
    
    print(f"Running benchmarks ({index_config} configuration, {scale} dataset)...")
    print(f"Database: {DEFAULT_CONFIG['database']} @ {DEFAULT_CONFIG['host']}:{DEFAULT_CONFIG['port']}")
    print(f"Warmup runs: {warmup_runs}, Measurement runs: {measurement_runs}")
    print()
    
    # Parse queries
    queries = parse_queries(queries_file)
    print(f"Found {len(queries)} queries to benchmark")
    print()
    
    # Connect to database
    conn = get_db_connection()
    
    try:
        query_results = []
        total_start_time = time.perf_counter()
        
        for query_num, description, sql_query in queries:
            print(f"Query {query_num}: {description}")
            
            # Run benchmark
            stats = run_benchmark(conn, sql_query, query_num, warmup_runs, measurement_runs)
            
            # Display summary
            print(f"  Results: min={stats['min']:.2f}ms, mean={stats['mean']:.2f}ms, "
                  f"p50={stats['p50']:.2f}ms, p95={stats['p95']:.2f}ms, max={stats['max']:.2f}ms")
            print()
            
            query_results.append({
                "query_number": query_num,
                "description": description,
                "query": sql_query,
                "statistics": stats
            })
        
        total_end_time = time.perf_counter()
        total_duration = total_end_time - total_start_time
        
        # Prepare results structure
        results = {
            "metadata": {
                "scale": scale,
                "index_configuration": index_config,
                "warmup_runs": warmup_runs,
                "measurement_runs": measurement_runs,
                "database": DEFAULT_CONFIG["database"],
                "host": DEFAULT_CONFIG["host"],
                "port": DEFAULT_CONFIG["port"],
                "total_queries": len(queries),
                "total_duration_seconds": total_duration,
                "captured_at": datetime.now().isoformat(),
            },
            "queries": query_results
        }
        
        # Save results
        output_filename = f"latency_{index_config}_{scale}.json"
        output_file = RESULTS_DIR / output_filename
        save_results_json(results, output_file)
        
        # Also save CSV summary
        csv_filename = f"latency_{index_config}_{scale}.csv"
        csv_file = RESULTS_DIR / csv_filename
        save_results_csv(results, csv_file)
        
        # Summary
        print("=" * 60)
        print(f"Benchmark complete!")
        print(f"  Total queries: {len(queries)}")
        print(f"  Total duration: {total_duration:.2f} seconds")
        print(f"  Results saved to: {output_file}")
        print(f"  Summary CSV saved to: {csv_file}")
        
        return True
        
    finally:
        conn.close()


def worker_execute_query(query: str, worker_id: int) -> Tuple[int, float, bool]:
    """
    Worker function to execute a query in a separate thread.
    Each worker has its own database connection.
    
    Args:
        query: SQL query to execute
        worker_id: Worker identifier
    
    Returns:
        Tuple of (worker_id, execution_time_ms, success)
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        t0 = time.perf_counter()
        cur.execute(query)
        cur.fetchall()  # Fetch all results
        t1 = time.perf_counter()
        
        cur.close()
        execution_time = (t1 - t0) * 1000  # Convert to milliseconds
        return (worker_id, execution_time, True)
        
    except Exception as e:
        print(f"  Worker {worker_id} error: {e}")
        return (worker_id, 0.0, False)
    finally:
        if conn:
            conn.close()


def run_throughput_benchmark(scale: str = "small", index_config: str = "no_index",
                             concurrency: int = DEFAULT_CONCURRENCY,
                             duration_seconds: int = DEFAULT_DURATION_SECONDS):
    """
    Run throughput benchmark with concurrent workers.
    
    Args:
        scale: Dataset scale (small, medium, large)
        index_config: Index configuration (no_index, with_index)
        concurrency: Number of concurrent workers (4-16)
        duration_seconds: Duration of the benchmark in seconds (20-60)
    """
    queries_file = SQL_DIR / "queries.sql"
    
    # Validate parameters
    if concurrency < 1 or concurrency > 16:
        print(f"Error: Concurrency must be between 1 and 16 (got {concurrency})")
        sys.exit(1)
    
    if duration_seconds < 20 or duration_seconds > 60:
        print(f"Error: Duration must be between 20 and 60 seconds (got {duration_seconds})")
        sys.exit(1)
    
    print(f"Running throughput benchmark ({index_config} configuration, {scale} dataset)...")
    print(f"Database: {DEFAULT_CONFIG['database']} @ {DEFAULT_CONFIG['host']}:{DEFAULT_CONFIG['port']}")
    print(f"Concurrency: {concurrency} workers")
    print(f"Duration: {duration_seconds} seconds")
    print()
    
    # Parse queries
    queries = parse_queries(queries_file)
    print(f"Found {len(queries)} queries to benchmark")
    print()
    
    # Use a mix of all queries for throughput testing
    # Round-robin through queries to simulate realistic workload
    query_results = []
    
    for query_num, description, sql_query in queries:
        print(f"Query {query_num}: {description}")
        print(f"  Running throughput test with {concurrency} workers for {duration_seconds}s...", end=" ", flush=True)
        
        # Track query execution statistics
        completed_queries = 0
        failed_queries = 0
        execution_times = []
        start_time = time.perf_counter()
        end_time = start_time + duration_seconds
        
        # Use ThreadPoolExecutor for concurrent execution
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            # Submit initial batch of queries
            futures = []
            query_counter = 0
            
            while time.perf_counter() < end_time:
                # Submit new queries to maintain concurrency
                while len(futures) < concurrency and time.perf_counter() < end_time:
                    future = executor.submit(worker_execute_query, sql_query, query_counter)
                    futures.append(future)
                    query_counter += 1
                
                # Check for completed queries
                for future in as_completed(futures[:]):
                    if time.perf_counter() >= end_time:
                        break
                    
                    try:
                        worker_id, exec_time, success = future.result(timeout=1.0)
                        if success:
                            completed_queries += 1
                            execution_times.append(exec_time)
                        else:
                            failed_queries += 1
                    except Exception as e:
                        failed_queries += 1
                    
                    futures.remove(future)
            
            # Wait for remaining queries to complete (with timeout)
            for future in futures:
                try:
                    worker_id, exec_time, success = future.result(timeout=5.0)
                    if success:
                        completed_queries += 1
                        execution_times.append(exec_time)
                    else:
                        failed_queries += 1
                except Exception:
                    failed_queries += 1
        
        actual_duration = time.perf_counter() - start_time
        qps = completed_queries / actual_duration if actual_duration > 0 else 0
        
        # Calculate statistics
        if execution_times:
            timings_sorted = sorted(execution_times)
            
            def percentile(sorted_list, p):
                if not sorted_list:
                    return None
                index = int((p / 100.0) * len(sorted_list))
                index = min(index, len(sorted_list) - 1)
                return sorted_list[index]
            
            stats = {
                "total_queries": completed_queries,
                "failed_queries": failed_queries,
                "duration_seconds": actual_duration,
                "qps": qps,
                "raw_timings": execution_times[:1000],  # Store first 1000 for analysis
                "min": min(execution_times),
                "max": max(execution_times),
                "mean": statistics.mean(execution_times),
                "median": statistics.median(execution_times),
                "p50": statistics.median(execution_times),
                "p95": percentile(timings_sorted, 95),
                "p99": percentile(timings_sorted, 99),
                "stddev": statistics.stdev(execution_times) if len(execution_times) > 1 else 0.0,
            }
        else:
            stats = {
                "total_queries": 0,
                "failed_queries": failed_queries,
                "duration_seconds": actual_duration,
                "qps": 0.0,
                "raw_timings": [],
                "min": None,
                "max": None,
                "mean": None,
                "median": None,
                "p50": None,
                "p95": None,
                "p99": None,
                "stddev": 0.0,
            }
        
        print(f"Done")
        print(f"  Completed: {completed_queries} queries, Failed: {failed_queries}")
        print(f"  QPS: {qps:.2f}, Duration: {actual_duration:.2f}s")
        if execution_times:
            print(f"  Latency: mean={stats['mean']:.2f}ms, p50={stats['p50']:.2f}ms, p95={stats['p95']:.2f}ms")
        print()
        
        query_results.append({
            "query_number": query_num,
            "description": description,
            "query": sql_query,
            "statistics": stats
        })
    
    # Prepare results structure
    total_queries = sum(q["statistics"]["total_queries"] for q in query_results)
    total_qps = sum(q["statistics"]["qps"] for q in query_results)
    
    results = {
        "metadata": {
            "scale": scale,
            "index_configuration": index_config,
            "concurrency": concurrency,
            "duration_seconds": duration_seconds,
            "database": DEFAULT_CONFIG["database"],
            "host": DEFAULT_CONFIG["host"],
            "port": DEFAULT_CONFIG["port"],
            "total_queries": len(queries),
            "total_completed_queries": total_queries,
            "total_qps": total_qps,
            "avg_qps_per_query": total_qps / len(queries) if queries else 0,
            "captured_at": datetime.now().isoformat(),
        },
        "queries": query_results
    }
    
    # Save results
    output_filename = f"throughput_{index_config}_{scale}.json"
    output_file = RESULTS_DIR / output_filename
    save_results_json(results, output_file)
    
    # Summary
    print("=" * 60)
    print(f"Throughput benchmark complete!")
    print(f"  Total queries tested: {len(queries)}")
    print(f"  Total completed: {total_queries}")
    print(f"  Total QPS: {total_qps:.2f}")
    print(f"  Average QPS per query: {total_qps / len(queries):.2f}" if queries else "  Average QPS per query: 0.00")
    print(f"  Results saved to: {output_file}")
    
    return True


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Run latency or throughput benchmarks for all queries"
    )
    parser.add_argument(
        "--scale",
        type=str,
        default="small",
        choices=["small", "medium", "large"],
        help="Dataset scale (default: small)"
    )
    parser.add_argument(
        "--index-config",
        type=str,
        default="no_index",
        help="Index configuration (default: no_index). Use 'with_index' after applying indexes."
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help=f"Enable throughput mode with N concurrent workers (4-16). If not specified, runs latency benchmark."
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION_SECONDS,
        help=f"Duration in seconds for throughput benchmark (20-60, default: {DEFAULT_DURATION_SECONDS})"
    )
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=DEFAULT_WARMUP_RUNS,
        help=f"Number of warmup runs per query for latency benchmark (default: {DEFAULT_WARMUP_RUNS})"
    )
    parser.add_argument(
        "--measurement-runs",
        type=int,
        default=DEFAULT_MEASUREMENT_RUNS,
        help=f"Number of measurement runs per query for latency benchmark (default: {DEFAULT_MEASUREMENT_RUNS})"
    )
    
    args = parser.parse_args()
    
    # If concurrency is specified, run throughput benchmark
    if args.concurrency is not None:
        if run_throughput_benchmark(
            scale=args.scale,
            index_config=args.index_config,
            concurrency=args.concurrency,
            duration_seconds=args.duration
        ):
            sys.exit(0)
        else:
            sys.exit(1)
    else:
        # Otherwise, run latency benchmark
        if run_benchmarks(
            scale=args.scale,
            index_config=args.index_config,
            warmup_runs=args.warmup_runs,
            measurement_runs=args.measurement_runs
        ):
            sys.exit(0)
        else:
            sys.exit(1)


if __name__ == "__main__":
    main()

