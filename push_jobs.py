"""
Job Producer
Pushes all parameter combinations to Azure Queue.
Run this once before starting workers.
"""

import os
import json
from itertools import product
from azure.storage.queue import QueueClient
from azure.storage.blob import BlobServiceClient

# Config
STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
QUEUE_NAME = "backtest-jobs"
DATA_CONTAINER = "raw-data"

# Parameter ranges
FAST_RANGE = list(range(5, 101, 5))      # [5, 10, 15, ... 100] - 20 values
SLOW_RANGE = list(range(20, 301, 10))    # [20, 30, 40, ... 300] - 29 values

def get_available_coins():
    """List all parquet files in blob storage."""
    blob_service = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(DATA_CONTAINER)
    
    coins = []
    for blob in container_client.list_blobs():
        if blob.name.endswith('.parquet'):
            coin = blob.name.replace('.parquet', '')
            coins.append(coin)
    
    return coins

def generate_jobs(coins: list) -> list:
    """Generate all parameter combinations."""
    jobs = []
    
    for coin in coins:
        for fast, slow in product(FAST_RANGE, SLOW_RANGE):
            if fast < slow:  # fast MA must be shorter than slow MA
                jobs.append({
                    "coin": coin,
                    "fast_ma": fast,
                    "slow_ma": slow
                })
    
    return jobs

def push_jobs_to_queue(jobs: list):
    """Push all jobs to Azure Queue."""
    queue_client = QueueClient.from_connection_string(STORAGE_CONNECTION_STRING, QUEUE_NAME)
    
    print(f"Pushing {len(jobs)} jobs to queue...")
    
    for i, job in enumerate(jobs):
        queue_client.send_message(json.dumps(job))
        
        if (i + 1) % 100 == 0:
            print(f"  Pushed {i + 1}/{len(jobs)}")
    
    print(f"Done. {len(jobs)} jobs in queue.")

def main():
    if not STORAGE_CONNECTION_STRING:
        print("ERROR: AZURE_STORAGE_CONNECTION_STRING environment variable not set")
        print("Get it from: Azure Portal → Storage Account → Access keys")
        exit(1)
    
    # Get coins from blob storage
    print("Fetching available coins from blob storage...")
    coins = get_available_coins()
    print(f"Found {len(coins)} coins: {coins}")
    
    # Generate jobs
    jobs = generate_jobs(coins)
    print(f"Generated {len(jobs)} jobs")
    print(f"  {len(coins)} coins × {len(FAST_RANGE)} fast × {len(SLOW_RANGE)} slow params")
    
    # Push to queue
    push_jobs_to_queue(jobs)
    
    print("\nNext step: Start workers on VMSS to process the queue.")

if __name__ == "__main__":
    main()
