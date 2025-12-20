#!/usr/bin/env python3
"""
Complete benchmark runner that executes the full benchmark suite.
Reads configuration from config.yaml for reproducibility.
"""

import os
import sys
import yaml
import subprocess
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_FILE = PROJECT_ROOT / "config.yaml"


def load_config():
    """Load configuration from config.yaml."""
    if not CONFIG_FILE.exists():
        print(f"Error: Configuration file not found: {CONFIG_FILE}")
        sys.exit(1)
    
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading config.yaml: {e}")
        sys.exit(1)


def set_env_vars(config):
    """Set environment variables from config."""
    db_config = config.get("database", {})
    os.environ["DB_HOST"] = db_config.get("host", "localhost")
    os.environ["DB_PORT"] = str(db_config.get("port", 5432))
    os.environ["DB_NAME"] = db_config.get("name", "benchmark_db")
    os.environ["DB_USER"] = db_config.get("user", "benchmark")
    os.environ["DB_PASSWORD"] = db_config.get("password", "benchmark")


def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd)}")
    print()
    
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    
    if result.returncode != 0:
        print(f"\nError: {description} failed with exit code {result.returncode}")
        return False
    
    return True


def generate_data(config):
    """Generate data for all scales."""
    scales = config.get("benchmarks", {}).get("scales", ["small"])
    
    print("\n" + "="*60)
    print("STEP 1: Generate Data")
    print("="*60)
    
    for scale in scales:
        if not run_command(
            ["python", "data/raw/generate_data.py", "--scale", scale],
            f"Generating {scale} dataset"
        ):
            return False
    
    return True


def load_data(config):
    """Load data into database for all scales."""
    scales = config.get("benchmarks", {}).get("scales", ["small"])
    
    print("\n" + "="*60)
    print("STEP 2: Load Data into Database")
    print("="*60)
    
    for scale in scales:
        if not run_command(
            ["python", "benchmarks/load_data.py", "--scale", scale],
            f"Loading {scale} dataset"
        ):
            return False
    
    return True


def run_latency_benchmarks(config):
    """Run latency benchmarks for all scales and index configurations."""
    scales = config.get("benchmarks", {}).get("scales", ["small"])
    index_configs = config.get("benchmarks", {}).get("index_configs", ["no_index", "with_index"])
    bench_config = config.get("benchmarks", {}).get("latency", {})
    warmup = bench_config.get("warmup_runs", 2)
    measurement = bench_config.get("measurement_runs", 10)
    
    print("\n" + "="*60)
    print("STEP 3: Run Latency Benchmarks")
    print("="*60)
    
    for scale in scales:
        for index_config in index_configs:
            if index_config == "with_index":
                # Apply indexes first
                if not run_command(
                    ["python", "benchmarks/apply_indexes.py", "apply"],
                    f"Applying indexes for {scale} scale"
                ):
                    return False
            
            if not run_command(
                ["python", "benchmarks/run_benchmarks.py", 
                 "--scale", scale,
                 "--index-config", index_config,
                 "--warmup-runs", str(warmup),
                 "--measurement-runs", str(measurement)],
                f"Running latency benchmark ({index_config}, {scale})"
            ):
                return False
    
    return True


def run_throughput_benchmarks(config):
    """Run throughput benchmarks."""
    scales = config.get("benchmarks", {}).get("scales", ["small"])
    bench_config = config.get("benchmarks", {}).get("throughput", {})
    concurrency_levels = bench_config.get("concurrency_levels", [4, 8])
    duration = bench_config.get("duration_seconds", 30)
    
    print("\n" + "="*60)
    print("STEP 4: Run Throughput Benchmarks")
    print("="*60)
    
    # Ensure indexes are applied
    if not run_command(
        ["python", "benchmarks/apply_indexes.py", "apply"],
        "Applying indexes for throughput benchmarks"
    ):
        return False
    
    for scale in scales:
        for concurrency in concurrency_levels:
            if not run_command(
                ["python", "benchmarks/run_benchmarks.py",
                 "--scale", scale,
                 "--index-config", "with_index",
                 "--concurrency", str(concurrency),
                 "--duration", str(duration)],
                f"Running throughput benchmark ({scale}, {concurrency} workers)"
            ):
                return False
    
    return True


def capture_explain_plans(config):
    """Capture EXPLAIN plans for all configurations."""
    scales = config.get("benchmarks", {}).get("scales", ["small"])
    index_configs = config.get("benchmarks", {}).get("index_configs", ["no_index", "with_index"])
    
    print("\n" + "="*60)
    print("STEP 5: Capture EXPLAIN Plans")
    print("="*60)
    
    for index_config in index_configs:
        if index_config == "with_index":
            # Apply indexes first
            if not run_command(
                ["python", "benchmarks/apply_indexes.py", "apply"],
                "Applying indexes for EXPLAIN plans"
            ):
                return False
        else:
            # Drop indexes for no_index baseline
            if not run_command(
                ["python", "benchmarks/apply_indexes.py", "drop"],
                "Dropping indexes for baseline"
            ):
                return False
        
        for scale in scales:
            if not run_command(
                ["python", "benchmarks/explain.py", "--index-config", index_config],
                f"Capturing EXPLAIN plans ({index_config}, {scale})"
            ):
                return False
    
    return True


def analyze_results(config):
    """Run analysis and generate plots."""
    analysis_config = config.get("analysis", {})
    
    print("\n" + "="*60)
    print("STEP 6: Analyze Results")
    print("="*60)
    
    if analysis_config.get("generate_summary", True):
        if not run_command(
            ["python", "benchmarks/analyze_results.py"],
            "Generating summary CSV"
        ):
            return False
    
    if analysis_config.get("generate_plots", True):
        if not run_command(
            ["python", "benchmarks/plot_results.py"],
            "Generating plots"
        ):
            return False
    
    return True


def main():
    """Main function to run complete benchmark suite."""
    print("="*60)
    print("SQL Query Optimization Benchmarking - Full Suite")
    print("="*60)
    print(f"Configuration file: {CONFIG_FILE}")
    print()
    
    # Load configuration
    config = load_config()
    set_env_vars(config)
    
    # Run benchmark steps
    steps = [
        ("Generate Data", generate_data),
        ("Load Data", load_data),
        ("Run Latency Benchmarks", run_latency_benchmarks),
        ("Run Throughput Benchmarks", run_throughput_benchmarks),
        ("Capture EXPLAIN Plans", capture_explain_plans),
        ("Analyze Results", analyze_results),
    ]
    
    for step_name, step_func in steps:
        if not step_func(config):
            print(f"\n{'='*60}")
            print(f"Benchmark suite failed at step: {step_name}")
            print(f"{'='*60}")
            sys.exit(1)
    
    print("\n" + "="*60)
    print("Benchmark suite completed successfully!")
    print("="*60)
    print("\nResults available in:")
    print(f"  - Metrics: {PROJECT_ROOT / 'results' / 'metrics'}")
    print(f"  - Figures: {PROJECT_ROOT / 'results' / 'figures'}")
    print(f"  - Summary: {PROJECT_ROOT / 'results' / 'metrics' / 'summary.csv'}")


if __name__ == "__main__":
    main()

