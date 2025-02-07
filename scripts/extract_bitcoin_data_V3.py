
### Written by Dr. Keli Xiao
### V2 & V3 by Noah Tover
### DEPENDENCIES: Indexing is set to true in conf file. SQLite. Database Schema as of 12/9/2024
# Function specific dependencies in descriptions
### DESCRIPTION: Parses data into database schema. I didn't document much as the process is intuitive, simply a matter of reading in JSONs to relational database and calling the RPC when necessary.
### TODO: Make parallel. Note that rpc requests only single threaded- must be serialized.

import requests
import json
from time import time
import sqlite3  
import pandas as pd

# Arguments
chunksize = 5000 # Keep in mind that the ratio of txns per chunk grows exponentially as time goes on.
rpc_user = 'bbluser'
rpc_password = 'bblpassword'
rpc_url = 'http://127.0.0.1:8332/'
headers = {'content-type': 'text/plain;'}
progress_n = 100 # Prints progress every progess_n blocks
database_path = r"E:\transactions_database"

def rpc_request(method, params=[]):
    """
    Sends a JSON-RPC request to Bitcoin Core and returns the response.
    """
    payload = json.dumps({
        "jsonrpc": "1.0",
        "id": method,
        "method": method,
        "params": params
    })
    try:
        response = requests.post(rpc_url, headers=headers, data=payload, auth=(rpc_user, rpc_password))
        response.raise_for_status()  
        return response.json()
    except requests.exceptions.ConnectionError:
        print("Error: Unable to connect to Bitcoin Core. Please ensure it is running and check your configuration.")
        exit(1)
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
        exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)

def get_vout(block):
    """
    From a block JSON, parses the vout information into a dataframe. This allows chunked inserts and a speedup from V2.
    DEPENDENCY: The column names of this dataframe match those of the SQL schema (for inserts)
    DEPENDENCY: Bitcoin core RPC expected returns as of 2/3/2025
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
    df = df.reindex(columns = columns, fill_value = None)
    
    return df
# TODO: Ensure it can handle errors fine with duplicates.
def get_vin(data):
    """
    Gets vin information from a block and stores it in a dataframe. This helps with faster, chunked inserts.
    DEPENDENCY: The column names of this dataframe match those of the SQL schema (for inserts)
    DEPENDENCY: Bitcoin core RPC expected returns as of 2/3/25
    TODO: Merge with get_vout if needed for greater module depth.
    """
    # Flatten the "vin" entries for each transaction.
    # The parent's txid is included via the meta parameter.
    df = pd.json_normalize(
        data.get("tx", []),
        record_path="vin",
        meta=["txid"],
        errors="ignore",
        record_prefix="vin_"
    )
    
    # Rename nested columns; if a column does not exist, pandas ignores it.
    rename_map = {
        "vin_scriptSig.asm": "vin_asm",
        "vin_txinwitness": "witness_data"
    }
    
    df = df.rename(columns=rename_map)
    
    # Get only columns we want from dataframe. Fill those observations that don't exist with None.
    columns = ["txid", "vin_txid", "vin_vout", "vin_asm", "witness_data"]
    df = df.reindex(columns = columns, fill_value = None)
    df["witness_data"] = df["witness_data"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else None
    ) # Join the witness data into a csv. This is a temporary bandaid. TODO:: Identify a standard for witness txns and add a warning for that dependency.
       
    return df
                

def process_transactions(block, conn):
    for tx in block['tx']:
        txid = tx["txid"]
        # There is no cost feasible way to see when a Bitcoin transaction was created with certainty; these may be used as estimates though.
        median_blocktime = block.get("mediantime")
        miner_time = block.get("time")
        locktime = block.get("locktime", None)
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO transactions (txid, median_blocktime, miner_time, locktime)
                VALUES (?, ?, ?, ?)
                """, 
                (txid, median_blocktime, miner_time, locktime)
            )
            conn.commit()
        except Exception as e:
            # If that transaction is already parsed it wont be added to the database twice, no need to do anything.
            if "UNIQUE constraint failed" not in str(e):
                print(e)
                raise
            else:
                print(e)
        
        # Process inputs and outputs
    # DEPENDENCY: Inputs MUST come after outputs for foreign key adherence.
    vout_df = get_vout(block) 
    vin_df = get_vin(block) 
    # TODO: Collapse these two loops. Maybe wrap the exception.
    # If any error in insertion, loop over each row and print the error if its not acceptable.
    try:
        vout_df.to_sql("outputs", conn, if_exists = "append", index = False, method = 'multi')
        vin_df.to_sql("inputs", conn, if_exists = "append", index = False, method = 'multi')
    except:
        for _, each in vout_df.iterrows():
            try:
                each_frame = each.to_frame()
                each_frame = each_frame.T
                each_frame.to_sql("outputs", conn, if_exists = "append", index = False, method = 'multi')
            except Exception as e:
                if "UNIQUE constraint failed" not in str(e):
                    print(e)
                    raise
                else:
                    print(e)
        for _, each in vin_df.iterrows():
            try:
                each_frame = each.to_frame()
                each_frame = each_frame.T
                each_frame.to_sql("inputs", conn, if_exists = "append", index = False, method = 'multi')
            except Exception as e:
                if "UNIQUE constraint failed" not in str(e):
                    print(e)
                    raise
                else:
                    print(e)
        
                
        
    conn.commit()
    

def createBlockchainTxnDatabase(start_height, chunk_size=chunksize):
    conn = sqlite3.connect(database_path)
    conn.execute("PRAGMA foreign_keys = ON;")  
    
    end_height = rpc_request("getblockcount")['result']
    
    for chunk_start in range(start_height, end_height + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, end_height)
        
        for height in range(chunk_start, chunk_end + 1):
            blockhash = rpc_request("getblockhash", [height])['result']
            block = rpc_request("getblock", [blockhash, 2])['result']
            
            process_transactions(block, conn)
            
            if height % progress_n == 0:
                print(f"Processed block height: {height}", "at", time())
            
    conn.close()
    
def main():
    start_height = int(input("Enter the starting block height: "))
    createBlockchainTxnDatabase(start_height)

if __name__ == "__main__":
    main()


