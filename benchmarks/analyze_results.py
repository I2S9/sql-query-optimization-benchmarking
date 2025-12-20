#!/usr/bin/env python3
"""
Analysis script for SQL Query Optimization Benchmarking.
Aggregates benchmark results and generates comparative metrics.
"""

import json
import csv
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional

# Directories
RESULTS_DIR = Path(__file__).parent.parent / "results" / "metrics"

# Scales and configurations
SCALES = ["small", "medium", "large"]
INDEX_CONFIGS = ["no_index", "with_index"]


def load_latency_results(scale: str, index_config: str) -> Optional[Dict[str, Any]]:
    """
    Load latency benchmark results from JSON file.
    
    Args:
        scale: Dataset scale (small, medium, large)
        index_config: Index configuration (no_index, with_index)
    
    Returns:
        Dictionary with results or None if file doesn't exist
    """
    filename = f"latency_{index_config}_{scale}.json"
    filepath = RESULTS_DIR / filename
    
    if not filepath.exists():
        return None
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None


def calculate_speedup(no_index_value: float, with_index_value: float) -> Optional[float]:
    """
    Calculate speedup ratio (no_index / with_index).
    
    Args:
        no_index_value: Metric value without index
        with_index_value: Metric value with index
    
    Returns:
        Speedup ratio or None if calculation not possible
    """
    if with_index_value is None or with_index_value == 0:
        return None
    if no_index_value is None:
        return None
    return no_index_value / with_index_value


def extract_query_metrics(results: Dict[str, Any], query_num: int) -> Optional[Dict[str, float]]:
    """
    Extract metrics for a specific query from results.
    
    Args:
        results: Results dictionary from JSON file
        query_num: Query number to extract
    
    Returns:
        Dictionary with metrics or None if query not found
    """
    if "queries" not in results:
        return None
    
    for query_result in results["queries"]:
        if query_result.get("query_number") == query_num:
            stats = query_result.get("statistics", {})
            return {
                "p50": stats.get("p50"),
                "p95": stats.get("p95"),
                "mean": stats.get("mean"),
                "min": stats.get("min"),
                "max": stats.get("max"),
            }
    
    return None


def analyze_results():
    """
    Analyze all benchmark results and generate summary CSV.
    """
    print("Analyzing benchmark results...")
    print(f"Results directory: {RESULTS_DIR}")
    print()
    
    # Collect all available results
    all_results = {}
    
    for scale in SCALES:
        all_results[scale] = {}
        for index_config in INDEX_CONFIGS:
            results = load_latency_results(scale, index_config)
            if results:
                all_results[scale][index_config] = results
                print(f"Loaded: latency_{index_config}_{scale}.json")
            else:
                print(f"Missing: latency_{index_config}_{scale}.json")
    
    print()
    
    # Extract all query numbers from available results
    query_numbers = set()
    for scale in SCALES:
        for index_config in INDEX_CONFIGS:
            if scale in all_results and index_config in all_results[scale]:
                results = all_results[scale][index_config]
                if "queries" in results:
                    for query_result in results["queries"]:
                        query_numbers.add(query_result.get("query_number"))
    
    if not query_numbers:
        print("Error: No benchmark results found!")
        return False
    
    query_numbers = sorted(query_numbers)
    print(f"Found {len(query_numbers)} queries across all results")
    print()
    
    # Prepare summary data
    summary_rows = []
    
    for scale in SCALES:
        for query_num in query_numbers:
            # Get metrics for both configurations
            no_index_metrics = None
            with_index_metrics = None
            
            if scale in all_results and "no_index" in all_results[scale]:
                no_index_metrics = extract_query_metrics(
                    all_results[scale]["no_index"], query_num
                )
            
            if scale in all_results and "with_index" in all_results[scale]:
                with_index_metrics = extract_query_metrics(
                    all_results[scale]["with_index"], query_num
                )
            
            # Get query description
            description = None
            if scale in all_results:
                for config in INDEX_CONFIGS:
                    if config in all_results[scale]:
                        results = all_results[scale][config]
                        if "queries" in results:
                            for query_result in results["queries"]:
                                if query_result.get("query_number") == query_num:
                                    description = query_result.get("description", "")
                                    break
                        if description:
                            break
            
            # Calculate speedups
            p50_speedup = None
            p95_speedup = None
            mean_speedup = None
            
            if no_index_metrics and with_index_metrics:
                p50_speedup = calculate_speedup(
                    no_index_metrics.get("p50"),
                    with_index_metrics.get("p50")
                )
                p95_speedup = calculate_speedup(
                    no_index_metrics.get("p95"),
                    with_index_metrics.get("p95")
                )
                mean_speedup = calculate_speedup(
                    no_index_metrics.get("mean"),
                    with_index_metrics.get("mean")
                )
            
            # Build row
            row = {
                "scale": scale,
                "query_number": query_num,
                "description": description or "",
                "no_index_p50_ms": no_index_metrics.get("p50") if no_index_metrics else None,
                "no_index_p95_ms": no_index_metrics.get("p95") if no_index_metrics else None,
                "no_index_mean_ms": no_index_metrics.get("mean") if no_index_metrics else None,
                "with_index_p50_ms": with_index_metrics.get("p50") if with_index_metrics else None,
                "with_index_p95_ms": with_index_metrics.get("p95") if with_index_metrics else None,
                "with_index_mean_ms": with_index_metrics.get("mean") if with_index_metrics else None,
                "p50_speedup": p50_speedup,
                "p95_speedup": p95_speedup,
                "mean_speedup": mean_speedup,
            }
            
            summary_rows.append(row)
    
    # Write summary CSV
    output_file = RESULTS_DIR / "summary.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    fieldnames = [
        "scale",
        "query_number",
        "description",
        "no_index_p50_ms",
        "no_index_p95_ms",
        "no_index_mean_ms",
        "with_index_p50_ms",
        "with_index_p95_ms",
        "with_index_mean_ms",
        "p50_speedup",
        "p95_speedup",
        "mean_speedup",
    ]
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in summary_rows:
            # Format numeric values
            formatted_row = {}
            for key, value in row.items():
                if value is None:
                    formatted_row[key] = ""
                elif isinstance(value, float):
                    formatted_row[key] = f"{value:.2f}"
                else:
                    formatted_row[key] = value
            writer.writerow(formatted_row)
    
    print(f"Summary written to: {output_file}")
    print(f"Total rows: {len(summary_rows)}")
    
    # Print summary statistics
    print()
    print("=" * 60)
    print("Summary Statistics:")
    print()
    
    # Calculate average speedups per scale
    for scale in SCALES:
        scale_rows = [r for r in summary_rows if r["scale"] == scale]
        if not scale_rows:
            continue
        
        p50_speedups = [r["p50_speedup"] for r in scale_rows if r["p50_speedup"] is not None]
        p95_speedups = [r["p95_speedup"] for r in scale_rows if r["p95_speedup"] is not None]
        mean_speedups = [r["mean_speedup"] for r in scale_rows if r["mean_speedup"] is not None]
        
        if p50_speedups:
            avg_p50 = sum(p50_speedups) / len(p50_speedups)
            avg_p95 = sum(p95_speedups) / len(p95_speedups) if p95_speedups else None
            avg_mean = sum(mean_speedups) / len(mean_speedups) if mean_speedups else None
            
            print(f"{scale.upper()} scale:")
            print(f"  Average p50 speedup: {avg_p50:.2f}x")
            if avg_p95:
                print(f"  Average p95 speedup: {avg_p95:.2f}x")
            if avg_mean:
                print(f"  Average mean speedup: {avg_mean:.2f}x")
            print()
    
    return True


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Analyze benchmark results and generate summary CSV"
    )
    
    args = parser.parse_args()
    
    if analyze_results():
        return 0
    else:
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())

