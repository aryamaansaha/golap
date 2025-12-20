#!/usr/bin/env python3
"""
Compare memory usage: GOLAP streaming vs naive full-load approach
"""

import subprocess
import json
import time
import os
import sys
import psutil
import csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def load_csv_naive(csv_path):
    """Naive approach: load entire CSV into memory"""
    rows = []
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        for row in reader:
            rows.append(row)
    return rows

def measure_naive_memory(csv_path):
    """Measure memory when loading CSV naively"""
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / (1024 * 1024)  # MB
    
    data = load_csv_naive(csv_path)
    
    mem_after = process.memory_info().rss / (1024 * 1024)  # MB
    memory_used = mem_after - mem_before
    
    return memory_used, len(data)

def collect_golap_metrics(binary_path, query, testdir):
    """Collect GOLAP metrics"""
    import collect_metrics
    return collect_metrics.collect_metrics(binary_path, query, testdir, sample_interval=0.1)

def plot_comparison(golap_metrics, naive_memory, csv_size_mb, output_file='comparison.png'):
    """Plot comparison between GOLAP and naive approach"""
    
    fig = plt.figure(figsize=(14, 8))
    fig.suptitle('Memory Usage: GOLAP Streaming vs Naive Full-Load', fontsize=14, fontweight='bold')
    
    # Create grid: 2 rows, 2 columns
    # Top row: GOLAP time series (left) and Naive illustration (right)
    # Bottom row: Bar comparison (spans both columns)
    ax1 = fig.add_subplot(2, 2, 1)  # Top left - GOLAP
    ax2 = fig.add_subplot(2, 2, 2)  # Top right - Naive
    ax3 = fig.add_subplot(2, 1, 2)  # Bottom - Bar chart
    
    elapsed = golap_metrics['elapsed_times']
    memory = golap_metrics['memory_mb']
    peak_golap = max(memory) if memory else 0
    
    # Plot 1: GOLAP streaming (actual time series)
    ax1.plot(elapsed, memory, '#2E86AB', linewidth=2, marker='o', markersize=3)
    ax1.fill_between(elapsed, memory, alpha=0.3, color='#2E86AB')
    ax1.axhline(y=peak_golap, color='#2E86AB', linestyle='--', alpha=0.5)
    ax1.set_xlabel('Time (seconds)')
    ax1.set_ylabel('Memory (MB)')
    ax1.set_title(f'GOLAP Streaming\nPeak: {peak_golap:.1f}MB', fontsize=11, fontweight='bold', color='#2E86AB')
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(0, max(peak_golap * 1.5, 1))  # Scale to GOLAP's range
    
    # Plot 2: Naive load illustration (step function)
    if elapsed:
        max_time = max(elapsed) if max(elapsed) > 0 else 0.01
        naive_times = [0, 0, max_time]
        naive_mem = [0, naive_memory, naive_memory]
        ax2.plot(naive_times, naive_mem, '#A23B72', linewidth=2)
        ax2.fill_between([0, max_time], [naive_memory, naive_memory], alpha=0.3, color='#A23B72')
    ax2.axhline(y=naive_memory, color='#A23B72', linestyle='--', alpha=0.5)
    ax2.set_xlabel('Time (seconds)')
    ax2.set_ylabel('Memory (MB)')
    ax2.set_title(f'Naive Full-Load\nPeak: {naive_memory:.1f}MB', fontsize=11, fontweight='bold', color='#A23B72')
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(0, naive_memory * 1.2)  # Scale to Naive's range
    
    # Plot 3: Bar comparison
    approaches = ['GOLAP\n(Streaming)', 'Naive\n(Full Load)']
    memory_values = [peak_golap, naive_memory]
    colors = ['#2E86AB', '#A23B72']
    
    bars = ax3.bar(approaches, memory_values, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    ax3.set_ylabel('Peak Memory Usage (MB)', fontsize=11)
    ax3.set_title(f'Direct Comparison (CSV Size: {csv_size_mb:.1f}MB)', fontsize=12, fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for bar, val in zip(bars, memory_values):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height + naive_memory*0.02,
                f'{val:.1f}MB',
                ha='center', va='bottom', fontweight='bold', fontsize=11)
    
    # Add savings annotation
    savings = naive_memory - peak_golap
    savings_pct = (savings / naive_memory * 100) if naive_memory > 0 else 0
    ratio = naive_memory / peak_golap if peak_golap > 0 else 0
    
    ax3.text(0.5, naive_memory * 0.5, 
             f'GOLAP uses {ratio:.0f}x LESS memory\n({savings_pct:.1f}% reduction)',
             ha='center', va='center', fontsize=12, fontweight='bold',
             bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.9, edgecolor='darkgreen', linewidth=2))
    
    plt.tight_layout()
    plt.subplots_adjust(hspace=0.35)
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Comparison plot saved to: {output_file}")
    print(f"\nResults:")
    print(f"  CSV file size: {csv_size_mb:.1f}MB")
    print(f"  GOLAP peak memory: {peak_golap:.1f}MB")
    print(f"  Naive load memory: {naive_memory:.1f}MB")
    print(f"  Memory savings: {savings:.1f}MB ({savings_pct:.1f}%)")

def main():
    if len(sys.argv) < 4:
        print("Usage: python3 compare_approaches.py <binary> <csv_file> <query>")
        print("\nExample:")
        print('  python3 compare_approaches.py ../golap small_test.csv "SELECT COUNT(*) FROM \\`small_test.csv\\`"')
        sys.exit(1)
    
    binary_path = sys.argv[1]
    csv_file = sys.argv[2]
    query = sys.argv[3]
    
    # Get CSV file size
    csv_path = os.path.join('..', 'testdata', csv_file)
    csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    
    print(f"Comparing approaches for: {csv_file} ({csv_size_mb:.1f}MB)")
    print()
    
    # Measure naive approach
    print("Measuring naive full-load approach...")
    naive_memory, num_rows = measure_naive_memory(csv_path)
    print(f"  Loaded {num_rows:,} rows")
    print(f"  Memory used: {naive_memory:.1f}MB")
    print()
    
    # Measure GOLAP
    print("Measuring GOLAP streaming approach...")
    golap_metrics = collect_golap_metrics(binary_path, query, '../testdata')
    peak_golap = max(golap_metrics['memory_mb']) if golap_metrics['memory_mb'] else 0
    print(f"  Peak memory: {peak_golap:.1f}MB")
    print()
    
    # Plot comparison
    output_file = f"comparison_{csv_file.replace('.csv', '')}.png"
    plot_comparison(golap_metrics, naive_memory, csv_size_mb, output_file)

if __name__ == '__main__':
    main()

