#!/usr/bin/env python3
"""
Fair comparison: Go GOLAP streaming vs Go naive full-load
Both implementations in Go for apples-to-apples comparison
"""

import subprocess
import json
import time
import os
import sys
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# Import the metrics collector
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import collect_metrics

def run_naive_loader(binary_path, csv_path):
    """Run Go naive loader and extract memory usage"""
    try:
        result = subprocess.run(
            [binary_path, csv_path],
            capture_output=True,
            text=True,
            timeout=300
        )
        output = result.stdout
        
        # Parse MEMORY_MB from output
        match = re.search(r'MEMORY_MB=([\d.]+)', output)
        if match:
            memory_mb = float(match.group(1))
        else:
            # Fallback: parse from "Memory used (Alloc)"
            match = re.search(r'Memory used \(Alloc\): ([\d.]+) MB', output)
            memory_mb = float(match.group(1)) if match else 0
        
        # Parse ROWS
        match = re.search(r'ROWS=(\d+)', output)
        rows = int(match.group(1)) if match else 0
        
        # Parse TIME_MS
        match = re.search(r'TIME_MS=(\d+)', output)
        time_ms = int(match.group(1)) if match else 0
        
        return {
            'memory_mb': memory_mb,
            'rows': rows,
            'time_ms': time_ms,
            'exit_code': result.returncode
        }
    except Exception as e:
        return {'memory_mb': 0, 'rows': 0, 'time_ms': 0, 'exit_code': 1, 'error': str(e)}

def plot_go_comparison(golap_peak_mb, naive_mb, csv_size_mb, output_file='go_comparison.png'):
    """Create comparison plot between Go GOLAP and Go naive"""
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle('Memory Usage: Go GOLAP Streaming vs Go Naive Full-Load\n(Fair Comparison - Both in Go)', 
                 fontsize=13, fontweight='bold')
    
    # Plot 1: Bar comparison (linear scale)
    ax1 = axes[0]
    approaches = ['GOLAP\n(Streaming)', 'Go Naive\n(Full Load)']
    memory_values = [golap_peak_mb, naive_mb]
    colors = ['#2E86AB', '#E94F37']
    
    bars = ax1.bar(approaches, memory_values, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    ax1.set_ylabel('Peak Memory Usage (MB)', fontsize=11)
    ax1.set_title(f'Memory Comparison\n(CSV: {csv_size_mb:.1f}MB on disk)', fontsize=11)
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for bar, val in zip(bars, memory_values):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + max(memory_values)*0.02,
                f'{val:.1f}MB',
                ha='center', va='bottom', fontweight='bold', fontsize=11)
    
    # Add savings annotation
    savings = naive_mb - golap_peak_mb
    savings_pct = (savings / naive_mb * 100) if naive_mb > 0 else 0
    ratio = naive_mb / golap_peak_mb if golap_peak_mb > 0 else 0
    
    ax1.text(0.5, max(memory_values) * 0.5,
             f'GOLAP uses {ratio:.0f}x LESS memory\n({savings_pct:.1f}% reduction)',
             ha='center', va='center', fontsize=11, fontweight='bold',
             bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.9, edgecolor='darkgreen', linewidth=2))
    
    # Plot 2: Stacked comparison showing CSV size
    ax2 = axes[1]
    categories = ['CSV on Disk', 'GOLAP\n(Peak)', 'Go Naive\n(Peak)']
    values = [csv_size_mb, golap_peak_mb, naive_mb]
    colors2 = ['#888888', '#2E86AB', '#E94F37']
    
    bars2 = ax2.bar(categories, values, color=colors2, alpha=0.8, edgecolor='black', linewidth=2)
    ax2.set_ylabel('Size (MB)', fontsize=11)
    ax2.set_title('Memory vs File Size', fontsize=11)
    ax2.grid(True, alpha=0.3, axis='y')
    
    for bar, val in zip(bars2, values):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + max(values)*0.02,
                f'{val:.1f}MB',
                ha='center', va='bottom', fontweight='bold', fontsize=10)
    
    # Add expansion ratio annotation
    naive_expansion = naive_mb / csv_size_mb if csv_size_mb > 0 else 0
    golap_ratio = golap_peak_mb / csv_size_mb if csv_size_mb > 0 else 0
    
    ax2.text(0.98, 0.98, f'Go Naive: {naive_expansion:.1f}x file size\nGOLAP: {golap_ratio:.2f}x file size',
             transform=ax2.transAxes, ha='right', va='top', fontsize=9,
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9))
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Comparison plot saved to: {output_file}")

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    golap_binary = os.path.join(project_root, 'golap')
    naive_binary = os.path.join(project_root, 'naive_loader')
    testdir = os.path.join(project_root, 'testdata')
    csv_file = 'small_test.csv'
    csv_path = os.path.join(testdir, csv_file)
    
    # Check binaries exist
    if not os.path.exists(golap_binary):
        print(f"Error: {golap_binary} not found. Run 'go build -o golap .' first.")
        sys.exit(1)
    if not os.path.exists(naive_binary):
        print(f"Error: {naive_binary} not found. Run 'go build -o naive_loader ./cmd/naive_loader/' first.")
        sys.exit(1)
    
    # Get CSV file size
    csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    
    print("=" * 50)
    print("Go vs Go Comparison: GOLAP Streaming vs Naive Load")
    print("=" * 50)
    print(f"CSV file: {csv_file} ({csv_size_mb:.1f}MB)")
    print()
    
    # Run Go naive loader
    print("Running Go naive loader...")
    naive_result = run_naive_loader(naive_binary, csv_path)
    print(f"  Memory: {naive_result['memory_mb']:.2f}MB")
    print(f"  Rows: {naive_result['rows']:,}")
    print()
    
    # Run GOLAP and collect metrics
    print("Running GOLAP streaming...")
    query = f'SELECT COUNT(*), SUM(value) FROM `{csv_file}`'
    golap_metrics = collect_metrics.collect_metrics(golap_binary, query, testdir)
    golap_peak = max(golap_metrics['memory_mb']) if golap_metrics['memory_mb'] else 0
    print(f"  Peak memory: {golap_peak:.2f}MB")
    print()
    
    # Summary
    savings = naive_result['memory_mb'] - golap_peak
    savings_pct = (savings / naive_result['memory_mb'] * 100) if naive_result['memory_mb'] > 0 else 0
    ratio = naive_result['memory_mb'] / golap_peak if golap_peak > 0 else 0
    
    print("=" * 50)
    print("Results (Fair Go vs Go Comparison)")
    print("=" * 50)
    print(f"CSV file size:        {csv_size_mb:.1f}MB")
    print(f"Go Naive memory:      {naive_result['memory_mb']:.1f}MB ({naive_result['memory_mb']/csv_size_mb:.1f}x file size)")
    print(f"GOLAP peak memory:    {golap_peak:.1f}MB ({golap_peak/csv_size_mb:.2f}x file size)")
    print(f"Memory savings:       {savings:.1f}MB ({savings_pct:.1f}%)")
    print(f"GOLAP uses {ratio:.0f}x less memory than Go naive")
    print()
    
    # Generate plot
    output_file = os.path.join(script_dir, 'go_comparison.png')
    plot_go_comparison(golap_peak, naive_result['memory_mb'], csv_size_mb, output_file)

if __name__ == '__main__':
    main()

