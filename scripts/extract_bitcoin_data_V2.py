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

# Configuration
rpc_user = '<your_rpc_username>'
rpc_password = '<your_rpc_password>'
rpc_url = 'http://127.0.0.1:8332/'
headers = {'content-type': 'text/plain;'}

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
            "vin_scriptSig_hex": None,
            "vin_txinwitness": None,
            "vin_sequence": None
        }
    vin_list = []
    for vin_data in vin:
        scriptSig = vin_data.get("scriptSig", {})
        vin_list.append({
            "vin_txid": vin_data.get("txid", None),
            "vin_vout": vin_data.get("vout", None),
            "vin_scriptSig_asm": scriptSig.get("asm", None),
            "vin_scriptSig_hex": scriptSig.get("hex", None),
            "vin_txinwitness": json.dumps(vin_data.get("txinwitness", None)),
            "vin_sequence": vin_data.get("sequence", None)
        })
    return vin_list
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
            "vout_scriptPubKey_hex": None,
            "vout_scriptPubKey_address": None,
            "vout_scriptPubKey_type": None
        }

    vout_list = []
    for vout_data in vout:
        scriptPubKey = vout_data.get("scriptPubKey", {})
        vout_list.append({
            "vout_value": vout_data.get("value", None),
            "vout_n": vout_data.get("n", None),
            "vout_scriptPubKey_asm": scriptPubKey.get("asm", None),
            "vout_scriptPubKey_desc": scriptPubKey.get("desc", None),
            "vout_scriptPubKey_hex": scriptPubKey.get("hex", None),
            "vout_scriptPubKey_address": scriptPubKey.get("address", None),
            "vout_scriptPubKey_type": scriptPubKey.get("type", None)
        })
    return vout_list

def extract_blocks_and_transactions(start_height, chunk_size=10000):
    """
    Extracts block and transaction data from a specific start height to the latest block.
    """
    block_fieldnames = ["hash", "confirmations", "size", "height", "version", "merkleroot", "time", "nonce", "bits", "difficulty", "previousblockhash", "nextblockhash"]
    transaction_fieldnames = ["txid", "blockhash", "size", "version", "locktime",
                              "vin_txid", "vin_vout", "vin_scriptSig_asm", "vin_scriptSig_hex", "vin_txinwitness", "vin_sequence",
                              "vout_value", "vout_n", "vout_scriptPubKey_asm", "vout_scriptPubKey_desc", "vout_scriptPubKey_hex", "vout_scriptPubKey_address", "vout_scriptPubKey_type"]

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

                for vin_data in get_vin(tx.get("vin", [])):
                    for vout_data in get_vout(tx.get("vout", [])):
                        tx_entry = base_tx_data.copy()
                        tx_entry.update(vin_data)
                        tx_entry.update(vout_data)
                        transactions.append(tx_entry)
            # Print progress
            print(f"Processed block height: {height}")
            sleep(0.1)  # To avoid overloading the RPC server

        # Save the blocks and transactions data to CSV files for the current chunk
        blocks_filename = os.path.join(csv_folder, f'blocks_{chunk_start}_{chunk_end}.csv')
        transactions_filename = os.path.join(csv_folder, f'transactions_{chunk_start}_{chunk_end}.csv')
        
        save_to_csv(blocks_filename, block_fieldnames, blocks)
        save_to_csv(transactions_filename, transaction_fieldnames, transactions)

def main():
    # Define the starting block height for extraction
    start_height = int(input("Enter the starting block height: "))  # Modify as needed

    # Extract blocks and transactions from the specified start height to the latest block
    extract_blocks_and_transactions(start_height)

if __name__ == "__main__":
    main()


# In[ ]:





# In[ ]:




