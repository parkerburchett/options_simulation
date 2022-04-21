from threading import Thread
import concurrent.futures
import datetime
import pandas as pd
import numpy as np
from glob import glob

COLUMNS = ["symbol", "timestamp", "type", "strike_price", "expiration", "underlying_price", "bid_price", "bid_amount",
           "ask_price", "ask_amount", "mark_price", "mark_iv", "delta", "theta"]

COLUM_TYPES = {
    'symbol': str,
    'timestamp': int,
    'type': str,
    'strike_price': float,
    'expiration': int,
    'underlying_price': float,
    'bid_price': float,
    'bid_amount': float,
    'ask_price': float,
    'ask_amount': float,
    'mark_price': float,
    'delta': float,
    'theta': float
}

def get_files():
    files = glob("datasets/*.csv.gz")
    files.sort()
    return files

# no real reason to pick this threshold.
def filter_expiration(df, min=0, max=90):
    """Returns the subset of options where the time to expiration is between min and max"""
    return df[(df['days_to_expiration'] > datetime.timedelta(days=min)) & (
                df['days_to_expiration'] < datetime.timedelta(days=max))]


def _load_file_to_dataframe(file_path):
    df = pd.read_csv(file_path, compression='gzip', usecols=COLUMNS,
                    dtype=COLUM_TYPES, engine='c', float_precision="legacy")
                    
    eth_only = df[df['symbol'].str.contains("ETH")]
    eth_only['datetime'] = pd.to_datetime(eth_only['timestamp'] * 1000)
    eth_only['expiration_datetime'] = pd.to_datetime(eth_only['expiration'] * 1000)
    eth_only['days_to_expiration'] = eth_only['expiration_datetime'] - eth_only['datetime']
    eth_only.dropna(inplace=True)

    return filter_expiration(eth_only)


def reduce_data(a_file:str):
    save_path = f'reduced_datasets/reduced_{a_file.split("/")[1]}'
    df = _load_file_to_dataframe(a_file)
    df.to_csv(save_path, compression='gzip')
    return (save_path, df.shape)


def run():
    n_workers = 4
    # this is just limited by your memory.
    # I have 64 gigs of ram so I can have a couple going at once
    file_names = get_files()
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor: 
        futures = []
        for a_file in file_names:
            futures.append(executor.submit(reduce_data, a_file=a_file))
        for future in concurrent.futures.as_completed(futures):
            print(future.result())

run() 