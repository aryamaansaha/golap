#!/usr/bin/env python3
"""
Collect latency and memory metrics during GOLAP query execution
Outputs JSON data that can be plotted
"""

import subprocess
import json
import time
import os
import sys
import psutil
import signal

def get_process_memory(pid):
    """Get memory usage of a process in MB"""
    try:
        process = psutil.Process(pid)
        return process.memory_info().rss / (1024 * 1024)  # Convert to MB
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None

def collect_metrics(binary_path, query, testdir, sample_interval=0.1):
    """
    Run a query and collect memory/time metrics
    
    Returns:
        dict with 'timestamps', 'memory_mb', 'elapsed_times', 'total_time', 'exit_code'
    """
    metrics = {
        'timestamps': [],
        'memory_mb': [],
        'elapsed_times': [],
        'total_time': 0,
        'exit_code': 0,
        'query': query
    }
    
    # Change to test directory
    original_dir = os.getcwd()
    os.chdir(testdir)
    
    try:
        # Start the process
        start_time = time.time()
        process = subprocess.Popen(
            [binary_path, query],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid  # Create new process group
        )
        
        # Collect metrics while process is running
        while process.poll() is None:
            current_time = time.time()
            elapsed = current_time - start_time
            
            memory = get_process_memory(process.pid)
            
            metrics['timestamps'].append(current_time)
            metrics['elapsed_times'].append(elapsed)
            if memory is not None:
                metrics['memory_mb'].append(memory)
            else:
                metrics['memory_mb'].append(0)
            
            time.sleep(sample_interval)
        
        # Wait for process to complete
        stdout, stderr = process.communicate()
        end_time = time.time()
        
        metrics['total_time'] = end_time - start_time
        metrics['exit_code'] = process.returncode
        metrics['stdout'] = stdout.decode('utf-8', errors='ignore')
        metrics['stderr'] = stderr.decode('utf-8', errors='ignore')
        
    except KeyboardInterrupt:
        # Kill the process group
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        except:
            pass
        metrics['exit_code'] = -1
        metrics['total_time'] = time.time() - start_time
    finally:
        os.chdir(original_dir)
    
    return metrics

def main():
    if len(sys.argv) < 4:
        print("Usage: python3 collect_metrics.py <binary_path> <query> <testdir> [output_json]")
        print("\nExample:")
        print('  python3 collect_metrics.py ../golap "SELECT COUNT(*) FROM \\`large.csv\\`" ../testdata metrics.json')
        sys.exit(1)
    
    binary_path = sys.argv[1]
    query = sys.argv[2]
    testdir = sys.argv[3]
    output_file = sys.argv[4] if len(sys.argv) > 4 else 'metrics.json'
    
    print(f"Collecting metrics for query: {query}")
    print(f"Sampling every 0.1 seconds...")
    
    metrics = collect_metrics(binary_path, query, testdir)
    
    # Save to JSON
    with open(output_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"\nMetrics collected:")
    print(f"  Total time: {metrics['total_time']:.2f}s")
    print(f"  Samples: {len(metrics['elapsed_times'])}")
    print(f"  Peak memory: {max(metrics['memory_mb']):.2f}MB")
    print(f"  Exit code: {metrics['exit_code']}")
    print(f"\nData saved to: {output_file}")
    
    if metrics['exit_code'] != 0:
        print(f"\nQuery failed! Error:")
        print(metrics['stderr'])

if __name__ == '__main__':
    main()

