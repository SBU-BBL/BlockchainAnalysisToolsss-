############## WRITTEN BY NOAH TOVER ############################
import requests
from time import time, sleep
import pandas as pd
import os
# Developers Note #
# Witness data is stored as a comma separated string. 

### Arguments ###
chunksize = 800  # I've found experimentally that chunk sizes of 800 make the fastest copies into the database. 
rpc_user = 'bbluser'
rpc_password = 'bblpassword'
rpc_url = 'http://127.0.0.1:8332/'
headers = {'content-type': 'text/plain;'}
progress_n = 100000  # Prints progress every progess_n blocks

# Directory to store CSV output
csv_output_dir = r"D:\csv_dir"

os.makedirs(csv_output_dir, exist_ok=True)  # Create the directory if it doesn't exist

session = requests.Session()

def rpc_request(method, params=[]):
    payload = {
        "jsonrpc": "1.0",
        "id": method,
        "method": method,
        "params": params
    }
    max_attempts = 10
    attempt = 1
    # Sometimes the RPC gets overloaded by rate limits. Repeated attempts can help to solve that without setting the sleep too high. 
    try:
        r = session.post(
            rpc_url,
            headers=headers,
            json=payload,
            auth=(rpc_user, rpc_password)
        )
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        # This covers ConnectionError, HTTPError, Timeout, etc.
        print(f"RPC request error on attempt {attempt}: {e}")
        if attempt < max_attempts:
            print("Sleeping for 3 seconds, then retrying...")
            sleep(3)
            attempt += 1
        else:
            print("Failed 10 times. Exiting.")
            raise  
    except Exception as e:
        # Catch any other exceptions
        print(f"Unexpected error on attempt {attempt}: {e}")
        if attempt < max_attempts:
            print("Sleeping for 3 seconds, then retrying...")
            sleep(3)
            attempt += 1
        else:
            print("Failed 3 times. Exiting.")
            raise


# TODO: Merge these two functions into a deeper module.
def get_vout(block):
    """
    From a block JSON, parses the vout information into a DataFrame.
    """
    df = pd.json_normalize(block["tx"], record_path="vout", meta=["txid"], errors="ignore")
    
    df = df.rename(columns={
        "n": "vout_n",
        "value": "vout_value",
        "scriptPubKey.desc": "vout_scriptPubKey_desc",
        "scriptPubKey.address": "vout_scriptPubKey_address",
        "scriptPubKey.type": "vout_scriptPubKey_type"
    })
    
    columns = [
        "txid", "vout_n", "vout_value", 
        "vout_scriptPubKey_desc", "vout_scriptPubKey_address", "vout_scriptPubKey_type"
    ]
    df = df.reindex(columns=columns, fill_value=None)
    
    return df

def get_vin(block):
    """
    Gets vin information from a block and stores it in a DataFrame.
    """
    df = pd.json_normalize(
        block.get("tx", []),
        record_path="vin",
        meta=["txid"],
        errors="ignore",
        record_prefix="vin_"
    )
    
    # Rename nested columns
    rename_map = {
        "vin_scriptSig.asm": "vin_asm",
        "vin_txinwitness": "witness_data"
    }
    df = df.rename(columns=rename_map)
    
    columns = ["txid", "vin_txid", "vin_vout", "vin_asm", "witness_data"]
    df = df.reindex(columns=columns, fill_value=None)
    
    # Convert witness_data from list to a single comma-separated string. Please note that only the 1st item in witness data is the pubkey, but this portion of the code should be robust to changes in the Bitcoin protocol so it gets all.
    df["witness_data"] = df["witness_data"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else None
    )
       
    return df

def get_transactions(block):
    """
    Parses the transaction-level data for the block and returns a DataFrame
    with columns:
        txid, median_blocktime, miner_time, locktime
    """
    rows = []
    for tx in block['tx']:
        txid = tx["txid"]
        # approximate times
        median_blocktime = block.get("mediantime")
        miner_time = block.get("time")
        locktime = tx.get("locktime", None)
        rows.append({
            "txid": txid,
            "median_blocktime": median_blocktime,
            "miner_time": miner_time,
            "locktime": locktime
        })
    return pd.DataFrame(rows)

def createBlockchainCsv(start_height, end_height, chunk_size=chunksize):
    """
    Reads blocks from `start_height` until the chain tip, chunk by chunk,
    and writes three CSVs per chunk:
       transactions_chunk_X.csv
       inputs_chunk_X.csv
       outputs_chunk_X.csv
    """
    
    chunk_counter = 1
    
    for chunk_start in range(start_height, end_height + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, end_height)
        
        # Create empty dataframes that will hold the entire chunk's data
        chunk_transactions = []
        chunk_inputs = []
        chunk_outputs = []
        
        for height in range(chunk_start, chunk_end + 1):
            blockhash = rpc_request("getblockhash", [height])['result']
            block = rpc_request("getblock", [blockhash, 2])['result']

            # Collect transactions data
            tx_df = get_transactions(block)
            chunk_transactions.append(tx_df)

            # Collect vout (outputs) and vin (inputs)
            vout_df = get_vout(block)
            vin_df = get_vin(block)

            chunk_outputs.append(vout_df)
            chunk_inputs.append(vin_df)
            
            if height % progress_n == 0:
                print(f"Processed block height: {height}", "at time:", time())
        
        chunk_transactions_df = pd.concat(chunk_transactions, ignore_index=True)
        chunk_inputs_df = pd.concat(chunk_inputs, ignore_index=True)
        chunk_outputs_df = pd.concat(chunk_outputs, ignore_index=True)

        tx_csv_path = os.path.join(csv_output_dir, f"transactions_chunk_{chunk_counter}.csv")
        in_csv_path = os.path.join(csv_output_dir, f"inputs_chunk_{chunk_counter}.csv")
        out_csv_path = os.path.join(csv_output_dir, f"outputs_chunk_{chunk_counter}.csv")
        
        chunk_transactions_df.to_csv(tx_csv_path, index=False)
        chunk_inputs_df.to_csv(in_csv_path, index=False)
        chunk_outputs_df.to_csv(out_csv_path, index=False)
        
        print(f"Saved CSVs for chunk #{chunk_counter} (blocks {chunk_start} to {chunk_end}).")
        
        chunk_counter += 1

def main():
    start_height = int(input("Enter the starting block height: "))
    end_height = int(input("Enter the ending block height: "))
    createBlockchainCsv(start_height, end_height)
    signal_path = os.path.join(csv_output_dir, "done.signal")
    # This writes a file to the path to signal that this script has finished downloading csvs. The function "copy_csvs_to_postgre" relies on this to know when to stop waiting for that file to be filled.
    # Felt this was the most elegant way to signal any other dependencies while minimizing complexity.
    with open(signal_path, "w") as f:
        f.write("csv_download_complete")


if __name__ == "__main__":
    main()
