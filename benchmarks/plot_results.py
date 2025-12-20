#!/usr/bin/env python3
"""
Plotting script for SQL Query Optimization Benchmarking.
Generates simple graphs to visualize benchmark results.
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

# Use non-interactive backend
matplotlib.use('Agg')

# Directories
RESULTS_DIR = Path(__file__).parent.parent / "results" / "metrics"
FIGURES_DIR = Path(__file__).parent.parent / "results" / "figures"

# Scales and configurations
SCALES = ["small", "medium", "large"]
SCALE_ORDER = {"small": 0, "medium": 1, "large": 2}


def load_latency_results(scale: str, index_config: str) -> Optional[Dict[str, Any]]:
    """Load latency benchmark results from JSON file."""
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


def load_throughput_results(scale: str, index_config: str) -> Optional[Dict[str, Any]]:
    """Load throughput benchmark results from JSON file."""
    filename = f"throughput_{index_config}_{scale}.json"
    filepath = RESULTS_DIR / filename
    
    if not filepath.exists():
        return None
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None


def plot_latency_vs_scale():
    """Plot latency (p50) vs scale for both index configurations."""
    print("Generating latency vs scale graph...")
    
    scales_data = []
    no_index_p50 = []
    with_index_p50 = []
    
    for scale in SCALES:
        no_index_results = load_latency_results(scale, "no_index")
        with_index_results = load_latency_results(scale, "with_index")
        
        if no_index_results and with_index_results:
            # Calculate average p50 across all queries
            no_index_values = []
            with_index_values = []
            
            for query_result in no_index_results.get("queries", []):
                p50 = query_result.get("statistics", {}).get("p50")
                if p50 is not None:
                    no_index_values.append(p50)
            
            for query_result in with_index_results.get("queries", []):
                p50 = query_result.get("statistics", {}).get("p50")
                if p50 is not None:
                    with_index_values.append(p50)
            
            if no_index_values and with_index_values:
                scales_data.append(scale)
                no_index_p50.append(np.mean(no_index_values))
                with_index_p50.append(np.mean(with_index_values))
    
    if not scales_data:
        print("  No data available for latency vs scale")
        return False
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    x = np.arange(len(scales_data))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, no_index_p50, width, label='No Index', color='#d62728', alpha=0.8)
    bars2 = ax.bar(x + width/2, with_index_p50, width, label='With Index', color='#2ca02c', alpha=0.8)
    
    ax.set_xlabel('Dataset Scale', fontsize=12)
    ax.set_ylabel('Average Latency (ms, p50)', fontsize=12)
    ax.set_title('Query Latency vs Dataset Scale', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(scales_data)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}',
                   ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    
    output_file = FIGURES_DIR / "latency_vs_scale.png"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved to: {output_file}")
    return True


def plot_speedup_per_query():
    """Plot speedup per query for each scale."""
    print("Generating speedup per query graph...")
    
    # Collect speedup data
    speedup_data = {}
    
    for scale in SCALES:
        no_index_results = load_latency_results(scale, "no_index")
        with_index_results = load_latency_results(scale, "with_index")
        
        if no_index_results and with_index_results:
            speedups = []
            query_numbers = []
            
            # Match queries by query_number
            no_index_queries = {q.get("query_number"): q for q in no_index_results.get("queries", [])}
            with_index_queries = {q.get("query_number"): q for q in with_index_results.get("queries", [])}
            
            for query_num in sorted(set(no_index_queries.keys()) & set(with_index_queries.keys())):
                no_p50 = no_index_queries[query_num].get("statistics", {}).get("p50")
                with_p50 = with_index_queries[query_num].get("statistics", {}).get("p50")
                
                if no_p50 is not None and with_p50 is not None and with_p50 > 0:
                    speedup = no_p50 / with_p50
                    speedups.append(speedup)
                    query_numbers.append(query_num)
            
            if speedups:
                speedup_data[scale] = {
                    "query_numbers": query_numbers,
                    "speedups": speedups
                }
    
    if not speedup_data:
        print("  No data available for speedup per query")
        return False
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    x_offset = 0
    colors = {'small': '#1f77b4', 'medium': '#ff7f0e', 'large': '#2ca02c'}
    
    for scale in SCALES:
        if scale in speedup_data:
            data = speedup_data[scale]
            x = np.arange(len(data["query_numbers"])) + x_offset
            ax.bar(x, data["speedups"], width=0.25, label=f'{scale.capitalize()} scale', 
                  color=colors.get(scale, '#1f77b4'), alpha=0.7)
            x_offset += 0.25
    
    ax.set_xlabel('Query Number', fontsize=12)
    ax.set_ylabel('Speedup (no_index / with_index)', fontsize=12)
    ax.set_title('Speedup per Query by Scale', fontsize=14, fontweight='bold')
    
    # Set x-axis labels to query numbers
    if speedup_data:
        first_scale = list(speedup_data.keys())[0]
        query_numbers = speedup_data[first_scale]["query_numbers"]
        ax.set_xticks(np.arange(len(query_numbers)))
        ax.set_xticklabels(query_numbers)
    
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    ax.axhline(y=1.0, color='r', linestyle='--', alpha=0.5, linewidth=1)
    
    plt.tight_layout()
    
    output_file = FIGURES_DIR / "speedup_per_query.png"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved to: {output_file}")
    return True


def plot_throughput_vs_concurrency():
    """Plot throughput (QPS) vs concurrency level."""
    print("Generating throughput vs concurrency graph...")
    
    # Collect throughput data by scale and concurrency
    throughput_data = {}
    
    for scale in SCALES:
        results = load_throughput_results(scale, "with_index")
        if results:
            concurrency = results.get("metadata", {}).get("concurrency")
            total_qps = results.get("metadata", {}).get("total_qps")
            
            if concurrency is not None and total_qps is not None:
                if scale not in throughput_data:
                    throughput_data[scale] = {"concurrency": [], "qps": []}
                throughput_data[scale]["concurrency"].append(concurrency)
                throughput_data[scale]["qps"].append(total_qps)
    
    if not throughput_data:
        print("  No data available for throughput vs concurrency")
        return False
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    colors = {'small': '#1f77b4', 'medium': '#ff7f0e', 'large': '#2ca02c'}
    markers = {'small': 'o', 'medium': 's', 'large': '^'}
    
    for scale in SCALES:
        if scale in throughput_data:
            data = throughput_data[scale]
            if data["concurrency"] and data["qps"]:
                # Sort by concurrency
                sorted_data = sorted(zip(data["concurrency"], data["qps"]))
                concurrency_sorted, qps_sorted = zip(*sorted_data)
                
                ax.plot(concurrency_sorted, qps_sorted, 
                       marker=markers.get(scale, 'o'), 
                       label=f'{scale.capitalize()} scale',
                       color=colors.get(scale, '#1f77b4'),
                       linewidth=2, markersize=8)
    
    ax.set_xlabel('Concurrency (workers)', fontsize=12)
    ax.set_ylabel('Throughput (QPS)', fontsize=12)
    ax.set_title('Throughput vs Concurrency Level', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    output_file = FIGURES_DIR / "throughput_vs_concurrency.png"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved to: {output_file}")
    return True


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Generate graphs from benchmark results"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate all graphs (default)"
    )
    parser.add_argument(
        "--latency-scale",
        action="store_true",
        help="Generate latency vs scale graph"
    )
    parser.add_argument(
        "--speedup",
        action="store_true",
        help="Generate speedup per query graph"
    )
    parser.add_argument(
        "--throughput",
        action="store_true",
        help="Generate throughput vs concurrency graph"
    )
    
    args = parser.parse_args()
    
    # If no specific graph requested, generate all
    generate_all = args.all or not (args.latency_scale or args.speedup or args.throughput)
    
    print("Generating graphs from benchmark results...")
    print(f"Results directory: {RESULTS_DIR}")
    print(f"Figures directory: {FIGURES_DIR}")
    print()
    
    success_count = 0
    
    if generate_all or args.latency_scale:
        if plot_latency_vs_scale():
            success_count += 1
    
    if generate_all or args.speedup:
        if plot_speedup_per_query():
            success_count += 1
    
    if generate_all or args.throughput:
        if plot_throughput_vs_concurrency():
            success_count += 1
    
    print()
    print("=" * 60)
    print(f"Graph generation complete! Generated {success_count} graph(s)")
    print(f"Figures saved to: {FIGURES_DIR}")
    
    return 0 if success_count > 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())

