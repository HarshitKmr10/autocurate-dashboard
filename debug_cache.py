#!/usr/bin/env python3
"""Debug script to check and recreate analytics cache for a dataset."""

import asyncio
import sys
import os
from pathlib import Path

# Add the backend directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "backend"))

from backend.services.analytics_service import analytics_service


async def main():
    dataset_id = "1287a0d2-7e5d-428e-9440-30178d3a7c6f"
    file_path = f"data/uploads/{dataset_id}/ecommerce_orders.csv"
    
    print(f"Checking dataset: {dataset_id}")
    print(f"File path: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"ERROR: File {file_path} does not exist")
        return
    
    print(f"File exists: {os.path.exists(file_path)}")
    print(f"File size: {os.path.getsize(file_path)} bytes")
    
    # Check current analytics results
    print("\nChecking current analytics cache...")
    results = await analytics_service.get_analytics_results(dataset_id)
    if results:
        print("Analytics results found in cache!")
        print(f"Keys: {list(results.keys())}")
    else:
        print("No analytics results found in cache")
    
    # Try to reprocess
    print("\nStarting reprocessing...")
    try:
        new_results = await analytics_service.process_csv_file(
            dataset_id=dataset_id,
            file_path=file_path,
            sample_size=1000
        )
        print("Reprocessing completed successfully!")
        print(f"New results keys: {list(new_results.keys())}")
        
        # Check if now cached
        cached_results = await analytics_service.get_analytics_results(dataset_id)
        if cached_results:
            print("Results are now cached!")
        else:
            print("ERROR: Results were not cached properly")
            
    except Exception as e:
        print(f"ERROR during reprocessing: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
