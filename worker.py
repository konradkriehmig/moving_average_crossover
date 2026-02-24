"""
Backtest Worker
Runs on VMSS instances. Pulls jobs from Azure Queue, runs backtests, writes results to blob.
"""

import os
import json
import time
from azure.storage.queue import QueueClient
from azure.storage.blob import BlobServiceClient
import pandas as pd
import numpy as np
from io import BytesIO

# Config - set these as environment variables on VMSS
STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
QUEUE_NAME = "backtest-jobs"
DATA_CONTAINER = "raw-data"
RESULTS_CONTAINER = "backtest-results"

# Trading fee
TRADING_FEE = 0.001


def get_queue_client():
    """Connect to Azure Queue."""
    return QueueClient.from_connection_string(STORAGE_CONNECTION_STRING, QUEUE_NAME)

def get_blob_service():
    """Connect to Azure Blob Storage."""
    return BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

def load_price_data(blob_service, coin: str) -> pd.DataFrame:
    """Load price data from blob storage."""
    blob_client = blob_service.get_blob_client(DATA_CONTAINER, f"{coin}.parquet")
    
    stream = BytesIO()
    stream.write(blob_client.download_blob().readall())
    stream.seek(0)
    
    df = pd.read_parquet(stream)
    return df[["timestamp", "close"]]

def run_backtest(prices: pd.DataFrame, fast_ma: int, slow_ma: int) -> dict:
    """
    Run single MA crossover backtest.
    Returns dict with results.
    """
    df = prices.copy()
    df['fast'] = df['close'].rolling(window=fast_ma).mean()
    df['slow'] = df['close'].rolling(window=slow_ma).mean()
    df = df.dropna()
    
    if len(df) == 0:
        return {
            "total_return": 0.0,
            "num_trades": 0,
            "win_rate": 0.0
        }
    
    # Generate signals
    df['signal'] = 0
    df.loc[df['fast'] > df['slow'], 'signal'] = 1
    df.loc[df['fast'] < df['slow'], 'signal'] = -1
    
    # Track trades
    position = 0
    entry_price = 0
    trades = []
    
    for i, row in df.iterrows():
        if row['signal'] == 1 and position == 0:
            position = 1
            entry_price = row['close'] * (1 + TRADING_FEE)
        elif row['signal'] == -1 and position == 1:
            exit_price = row['close'] * (1 - TRADING_FEE)
            pnl = (exit_price - entry_price) / entry_price
            trades.append(pnl)
            position = 0
    
    if len(trades) == 0:
        return {
            "total_return": 0.0,
            "num_trades": 0,
            "win_rate": 0.0
        }
    
    total_return = np.prod([1 + t for t in trades]) - 1
    win_rate = sum(1 for t in trades if t > 0) / len(trades)
    
    return {
        "total_return": total_return,
        "num_trades": len(trades),
        "win_rate": win_rate
    }


def save_result(blob_service, result: dict):
    """Save result to blob storage."""
    blob_client = blob_service.get_blob_client(
        RESULTS_CONTAINER, 
        f"{result['coin']}_{result['fast_ma']}_{result['slow_ma']}.json"
    )
    
    blob_client.upload_blob(json.dumps(result), overwrite=True)


def process_job(blob_service, job: dict) -> dict:
    """Process a single backtest job."""
    coin = job["coin"]
    fast_ma = job["fast_ma"]
    slow_ma = job["slow_ma"]
    
    print(f"Processing: {coin} fast={fast_ma} slow={slow_ma}")
    
    # Load data
    prices = load_price_data(blob_service, coin)
    
    # Run backtest
    result = run_backtest(prices, fast_ma, slow_ma)
    
    # Add job info to result
    result["coin"] = coin
    result["fast_ma"] = fast_ma
    result["slow_ma"] = slow_ma
    
    return result


def worker_loop():
    """Main worker loop. Pulls jobs until queue is empty."""
    print("Worker starting...")
    print(f"Queue: {QUEUE_NAME}")
    print(f"Data container: {DATA_CONTAINER}")
    print(f"Results container: {RESULTS_CONTAINER}")
    
    queue_client = get_queue_client()
    blob_service = get_blob_service()
    
    jobs_processed = 0
    
    while True:
        # Get next message
        messages = queue_client.receive_messages(max_messages=1, visibility_timeout=300)
        
        message = None
        for msg in messages:
            message = msg
            break
        
        if message is None:
            print(f"Queue empty. Processed {jobs_processed} jobs. Exiting.")
            break
        
        try:
            # Parse job
            job = json.loads(message.content)
            
            # Process it
            result = process_job(blob_service, job)
            
            # Save result
            save_result(blob_service, result)
            
            # Delete message from queue
            queue_client.delete_message(message)
            
            jobs_processed += 1
            print(f"Completed job {jobs_processed}: {result['coin']} return={result['total_return']:.2%}")
            
        except Exception as e:
            print(f"Error processing job: {e}")
            # Message will become visible again after timeout
            continue
    
    print("Worker finished.")


if __name__ == "__main__":
    if not STORAGE_CONNECTION_STRING:
        print("ERROR: AZURE_STORAGE_CONNECTION_STRING environment variable not set")
        exit(1)
    
    worker_loop()
