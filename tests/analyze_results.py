#!/usr/bin/env python3
"""Analyze benchmark results and generate comparison reports."""

import sys
import csv
from pathlib import Path
from collections import defaultdict

def load_results(summary_csv):
    """Load results from summary CSV file."""
    results = []
    with open(summary_csv, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric fields
            row['hot_accounts'] = int(row['hot_accounts'])
            row['workers'] = int(row['workers'])
            row['throughput'] = float(row['throughput'])
            row['avg_latency_ms'] = float(row['avg_latency_ms'])
            row['transfers'] = int(row['transfers'])
            row['lookups'] = int(row['lookups'])
            row['twophase'] = int(row['twophase'])
            results.append(row)
    return results

def print_comparison_table(results, workload, workers):
    """Print comparison table for a specific workload and worker count."""
    print(f"\n{'='*80}")
    print(f"Workload: {workload.upper()}, Workers: {workers}")
    print(f"{'='*80}")
    print(f"{'Hot Accounts':<15} {'Redis (ops/s)':<20} {'TigerBeetle (ops/s)':<20} {'Speedup':<10}")
    print("-" * 80)

    # Group by hot_accounts
    hot_groups = defaultdict(dict)
    for r in results:
        if r['workload'] == workload and r['workers'] == workers:
            hot_groups[r['hot_accounts']][r['mode']] = r['throughput']

    for hot in sorted(hot_groups.keys()):
        redis_tps = hot_groups[hot].get('redis', 0)
        tb_tps = hot_groups[hot].get('tigerbeetle', 0)

        if tb_tps > 0:
            speedup = redis_tps / tb_tps
            speedup_str = f"{speedup:.2f}x"
        else:
            speedup_str = "N/A"

        print(f"{hot:<15} {redis_tps:<20.2f} {tb_tps:<20.2f} {speedup_str:<10}")

def print_scaling_analysis(results, mode, workload):
    """Print worker scaling analysis for a specific mode and workload."""
    print(f"\n{'='*80}")
    print(f"{mode.upper()} - {workload.upper()} - Worker Scaling Analysis")
    print(f"{'='*80}")
    print(f"{'Hot Accounts':<15} {'1 worker':<15} {'2 workers':<15} {'4 workers':<15} {'8 workers':<15}")
    print("-" * 80)

    # Group by hot_accounts and workers
    scaling_data = defaultdict(dict)
    for r in results:
        if r['mode'] == mode and r['workload'] == workload:
            scaling_data[r['hot_accounts']][r['workers']] = r['throughput']

    for hot in sorted(scaling_data.keys()):
        worker_data = scaling_data[hot]
        w1 = worker_data.get(1, 0)
        w2 = worker_data.get(2, 0)
        w4 = worker_data.get(4, 0)
        w8 = worker_data.get(8, 0)

        print(f"{hot:<15} {w1:<15.0f} {w2:<15.0f} {w4:<15.0f} {w8:<15.0f}")

def print_hot_account_impact(results, mode, workers):
    """Print hot account impact analysis."""
    print(f"\n{'='*80}")
    print(f"{mode.upper()} - Workers: {workers} - Hot Account Impact")
    print(f"{'='*80}")
    print(f"{'Workload':<15} {'Hot=1':<15} {'Hot=10':<15} {'Hot=100':<15} {'Hot=1000':<15} {'Hot=10000':<15}")
    print("-" * 80)

    # Group by workload and hot_accounts
    impact_data = defaultdict(dict)
    for r in results:
        if r['mode'] == mode and r['workers'] == workers:
            impact_data[r['workload']][r['hot_accounts']] = r['throughput']

    for workload in sorted(impact_data.keys()):
        hot_data = impact_data[workload]
        h1 = hot_data.get(1, 0)
        h10 = hot_data.get(10, 0)
        h100 = hot_data.get(100, 0)
        h1000 = hot_data.get(1000, 0)
        h10000 = hot_data.get(10000, 0)

        print(f"{workload:<15} {h1:<15.0f} {h10:<15.0f} {h100:<15.0f} {h1000:<15.0f} {h10000:<15.0f}")

def print_latency_comparison(results, workload, hot_accounts):
    """Print latency comparison for a specific workload and hot account count."""
    print(f"\n{'='*80}")
    print(f"Latency Comparison - {workload.upper()} - Hot Accounts: {hot_accounts}")
    print(f"{'='*80}")
    print(f"{'Workers':<10} {'Redis (ms)':<20} {'TigerBeetle (ms)':<20} {'Difference':<15}")
    print("-" * 80)

    # Group by workers
    latency_data = defaultdict(dict)
    for r in results:
        if r['workload'] == workload and r['hot_accounts'] == hot_accounts:
            latency_data[r['workers']][r['mode']] = r['avg_latency_ms']

    for workers in sorted(latency_data.keys()):
        redis_lat = latency_data[workers].get('redis', 0)
        tb_lat = latency_data[workers].get('tigerbeetle', 0)
        diff = tb_lat - redis_lat

        print(f"{workers:<10} {redis_lat:<20.3f} {tb_lat:<20.3f} {diff:+.3f} ms")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze_results.py <benchmark_results_directory>")
        sys.exit(1)

    results_dir = Path(sys.argv[1])
    summary_csv = results_dir / "summary.csv"

    if not summary_csv.exists():
        print(f"Error: {summary_csv} not found")
        sys.exit(1)

    print("Loading results...")
    results = load_results(summary_csv)
    print(f"Loaded {len(results)} benchmark results")

    # Get unique workloads and worker counts
    workloads = sorted(set(r['workload'] for r in results))
    worker_counts = sorted(set(r['workers'] for r in results))
    modes = sorted(set(r['mode'] for r in results))

    # 1. Throughput comparison for each workload and worker count
    print("\n" + "="*80)
    print("THROUGHPUT COMPARISON (Redis vs TigerBeetle)")
    print("="*80)
    for workload in workloads:
        for workers in worker_counts:
            print_comparison_table(results, workload, workers)

    # 2. Worker scaling analysis
    print("\n" + "="*80)
    print("WORKER SCALING ANALYSIS")
    print("="*80)
    for mode in modes:
        for workload in workloads:
            print_scaling_analysis(results, mode, workload)

    # 3. Hot account impact
    print("\n" + "="*80)
    print("HOT ACCOUNT IMPACT ANALYSIS")
    print("="*80)
    for mode in modes:
        for workers in worker_counts:
            print_hot_account_impact(results, mode, workers)

    # 4. Latency comparison
    print("\n" + "="*80)
    print("LATENCY ANALYSIS")
    print("="*80)
    for workload in workloads:
        # Pick a representative hot account count
        for hot in [10, 100, 1000]:
            if any(r['hot_accounts'] == hot and r['workload'] == workload for r in results):
                print_latency_comparison(results, workload, hot)

    # 5. Summary statistics
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)

    for mode in modes:
        mode_results = [r for r in results if r['mode'] == mode]
        if mode_results:
            avg_throughput = sum(r['throughput'] for r in mode_results) / len(mode_results)
            max_throughput = max(r['throughput'] for r in mode_results)
            max_tp_config = next(r for r in mode_results if r['throughput'] == max_throughput)

            print(f"\n{mode.upper()}:")
            print(f"  Average Throughput: {avg_throughput:.2f} ops/sec")
            print(f"  Max Throughput: {max_throughput:.2f} ops/sec")
            print(f"    Config: {max_tp_config['workload']}, {max_tp_config['workers']} workers, {max_tp_config['hot_accounts']} hot accounts")

    # Overall speedup
    print("\n" + "="*80)
    print("OVERALL SPEEDUP (Redis / TigerBeetle)")
    print("="*80)

    # Calculate average speedup across all matching configs
    speedups = []
    for r_redis in results:
        if r_redis['mode'] == 'redis':
            # Find matching TigerBeetle result
            r_tb = next((r for r in results
                        if r['mode'] == 'tigerbeetle'
                        and r['workload'] == r_redis['workload']
                        and r['workers'] == r_redis['workers']
                        and r['hot_accounts'] == r_redis['hot_accounts']), None)
            if r_tb and r_tb['throughput'] > 0:
                speedup = r_redis['throughput'] / r_tb['throughput']
                speedups.append((r_redis['workload'], r_redis['workers'], r_redis['hot_accounts'], speedup))

    if speedups:
        avg_speedup = sum(s[3] for s in speedups) / len(speedups)
        print(f"\nAverage Speedup: {avg_speedup:.2f}x")

        # Best and worst speedup
        best = max(speedups, key=lambda x: x[3])
        worst = min(speedups, key=lambda x: x[3])

        print(f"\nBest Speedup: {best[3]:.2f}x")
        print(f"  Config: {best[0]}, {best[1]} workers, {best[2]} hot accounts")

        print(f"\nWorst Speedup: {worst[3]:.2f}x")
        print(f"  Config: {worst[0]}, {worst[1]} workers, {worst[2]} hot accounts")

if __name__ == "__main__":
    main()
