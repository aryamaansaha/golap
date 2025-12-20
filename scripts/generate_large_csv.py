#!/usr/bin/env python3
"""
Generate a large CSV file for testing OOM scenarios
Usage: python3 generate_large_csv.py <num_rows> <output_file>
"""

import sys
import random
import csv
import os

def generate_large_csv(num_rows, output_file):
    print(f"Generating {num_rows:,} rows to {output_file}...")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
    
    with open(output_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'value', 'category', 'amount', 'description'])
        
        categories = ['Electronics', 'Furniture', 'Books', 'Clothing', 'Food', 'Toys', 'Sports', 'Tools']
        
        for i in range(num_rows):
            row = [
                i,
                random.randint(1, 1000000),
                random.choice(categories),
                round(random.uniform(10.0, 10000.0), 2),
                f"Product description for item {i} with additional text data to increase row size"
            ]
            writer.writerow(row)
            
            if (i + 1) % 100000 == 0:
                file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
                print(f"  Written {i+1:,} rows... ({file_size_mb:.1f} MB)")

    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"Done! Generated {output_file} ({file_size_mb:.1f} MB, {num_rows:,} rows)")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python3 generate_large_csv.py <num_rows> <output_file>")
        print("\nExamples:")
        print("  python3 generate_large_csv.py 1000000 testdata/medium.csv    # ~50MB")
        print("  python3 generate_large_csv.py 10000000 testdata/large.csv     # ~500MB")
        print("  python3 generate_large_csv.py 100000000 testdata/huge.csv    # ~5GB")
        sys.exit(1)
    
    num_rows = int(sys.argv[1])
    output_file = sys.argv[2]
    
    generate_large_csv(num_rows, output_file)

