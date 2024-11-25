#!/usr/bin/env python
# coding: utf-8

# In[16]:
### Written by Dr. Keli Xiao
### Updated by Noah Tover

import os
import csv
import json
import requests
from time import sleep
import gzip
import shutil
import pandas as pd

# Configuration
rpc_user = '<your_rpc_username>'
rpc_password = '<your_rpc_password>'
rpc_url = 'http://127.0.0.1:8332/'
headers = {'content-type': 'text/plain;'}
progress_n = 100 # Prints progress every 100 blocks
# Folder to save CSV files
csv_folder = 'D:/bitcoin_data' # Update this path as needed
os.makedirs(csv_folder, exist_ok=True)

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
        response.raise_for_status()  # Raise an error for bad status codes
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

def save_to_csv(filename, fieldnames, rows):
    """
    Writes data to a CSV file.
    """
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
# Could be spedup by dragging duplicate rows down for vin & vout objects greater than length 1. To be done later.
def get_vin(vin):
    """
    Flattens the vin field.
    """
    if not vin:
        return {
            "vin_txid": None,
            "vin_vout": None,
            "vin_scriptSig_asm": None,
            "vin_txinwitness": None,
            "vin_sequence": None
        }
    for each in vin:
        each["scriptSig"] = each["scriptSig"]["asm"] # Only get the asm 
        
    vin_df = pd.DataFrame(vin)
    flattened_data = vin_df.to_dict('list') # Flatten unique vin objects to list
    return {
        "vin_txid": flattened_data.get("txid", None),
        "vin_vout": flattened_data.get("vout", None),
        "vin_scriptSig_asm": flattened_data.get("scriptSig", None),
        "vin_txinwitness": flattened_data.get("txinwitness", None), # TODO: Call RPC "decodescript" if this hex exists, then get address and asm from here
        "vin_sequence": flattened_data.get("sequence", None)
    }
#TODO: Collapse get_vout and get_vin into one module with a paste - dependencies.
def get_vout(vout):
    """
    Flattens the vout field.
    """
    if not vout:
        return {
            "vout_value": None,
            "vout_n": None,
            "vout_scriptPubKey_asm": None,
            "vout_scriptPubKey_desc": None,
            "vout_scriptPubKey_address": None,
            "vout_scriptPubKey_type": None
        }
    for each in vout:
        each["address"] = each["scriptPubKey"]["address"]
        each["asm"] = each["scriptPubKey"]["asm"] 
        each["type"] = each["scriptPubKey"]["type"]
        each["desc"] = each["scriptPubKey"]["desc"]
    vout_df = pd.DataFrame(vout)
    flattened_data = vout_df.to_dict('list') # Flatten unique vin objects to list
    return {
        "vout_value": flattened_data.get("value", None),
        "vout_n": flattened_data.get("n", None),
        "vout_scriptPubKey_asm": flattened_data.get("asm", None),
        "vout_scriptPubKey_desc": flattened_data.get("desc", None),
        "vout_scriptPubKey_address": flattened_data.get("address", None), 
        "vout_scriptPubKey_type": flattened_data.get("type", None)
    }

def extract_blocks_and_transactions(start_height, chunk_size=10000):
    """
    Extracts block and transaction data from a specific start height to the latest block.
    """
    block_fieldnames = ["hash", "confirmations", "size", "height", "version", "merkleroot", "time", "nonce", "bits", "difficulty", "previousblockhash", "nextblockhash"]
    end_height = rpc_request("getblockcount")['result']
    
    for chunk_start in range(start_height, end_height + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, end_height)
        blocks = []
        transactions = []

        for height in range(chunk_start, chunk_end + 1):
            blockhash = rpc_request("getblockhash", [height])['result']
            block = rpc_request("getblock", [blockhash, 2])['result']
            
            block_data = {key: block[key] for key in block_fieldnames if key in block}
            blocks.append(block_data)
            
            for tx in block['tx']:
                base_tx_data = {key: tx[key] for key in ["txid", "blockhash", "size", "version", "locktime"] if key in tx}
                base_tx_data['blockhash'] = blockhash

                tx_entry = base_tx_data.copy()
                tx_entry.update(get_vin(tx.get("vin", [])))
                tx_entry.update(get_vout(tx.get("vout", [])))
                transactions.append(tx_entry)
            # Print progress every progress_n blocks
            if height % progress_n == 0:
                print(f"Processed block height: {height}")
            sleep(0.1)  # To avoid overloading the RPC server

        # Save the blocks and transactions data to CSV files for the current chunk
        blocks_filename = os.path.join(csv_folder, f'blocks_{chunk_start}_{chunk_end}.csv')
        transactions_filename = os.path.join(csv_folder, f'transactions_{chunk_start}_{chunk_end}.json')
        
        save_to_csv(blocks_filename, block_fieldnames, blocks)
        with open(transactions_filename, 'w', encoding='utf-8') as jsonfile:
          json.dump(transactions, jsonfile)

def main():
    # Define the starting block height for extraction
    start_height = int(input("Enter the starting block height: "))  # Modify as needed

    # Extract blocks and transactions from the specified start height to the latest block
    extract_blocks_and_transactions(start_height)

if __name__ == "__main__":
    main()


# In[ ]:





# In[ ]:




