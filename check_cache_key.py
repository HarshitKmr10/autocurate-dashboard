#!/usr/bin/env python3
"""Check cache key for dataset."""

import hashlib

dataset_id = "1287a0d2-7e5d-428e-9440-30178d3a7c6f"
analytics_key = f"analytics:{dataset_id}"

# The cache service likely uses MD5 hashing for keys
cache_key = hashlib.md5(analytics_key.encode()).hexdigest()
print(f"Dataset ID: {dataset_id}")
print(f"Analytics key: {analytics_key}")
print(f"Expected cache key: {cache_key}")
