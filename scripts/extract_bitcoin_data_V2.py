### Written by Dr. Keli Xiao
### Updated by Noah Tover
### DEPENDENCIES: Indexing is set to true in conf file. SQLite. Database Schema as of 12/9/2024
### DESCRIPTION: Parses data into database schema. I didn't document much as the process is intuitive, simply a matter of reading in JSONs to relational database.
### TODO: Make parallel. Note that rpc requests only single threaded- must be serialized.
### DESCRIPTION:

import requests
import json
from time import sleep
import sqlite3  

# Arguments
chunksize = 5000 # Keep in mind that the ratio of txns per chunk grows exponentially as time goes on.
rpc_user = '<your_rpc_username>'
rpc_password = '<your_rpc_password>'
rpc_url = 'http://127.0.0.1:8332/'
headers = {'content-type': 'text/plain;'}
progress_n = 100 # Prints progress every progess_n blocks
database_path = "PATH HERE"

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

def get_vin(vin, txid, conn):
    if not vin:
        return
    for each in vin:
        vin_txid = each.get("txid")
        vin_vout = each.get("vout")
        vin_asm = each.get("scriptSig").get("asm", {})
        witness = each.get("txinwitness", [])
        # Collapse list of a strings into a single string with commas separating each.
        witness_data = ", ".join(witness) if witness else {}
        conn.execute(
            """
            INSERT INTO inputs (txid, vin_txid, vin_vout, vin_asm, witness_data)
            VALUES (?, ?, ?)
            """, 
            (txid, vin_txid, vin_vout, vin_asm, witness_data)
        )
        conn.commit()

        
def get_vout(vout, txid, conn):
    if not vout:
        return
    for each in vout:
        vout_n = each.get("n")
        vout_value = each.get("value")
        vout_scriptPubKey_desc = each.get("scriptPubKey", {}).get("desc")
        vout_scriptPubKey_address = each.get("scriptPubKey", {}).get("address")
        vout_scriptPubKey_type = each.get("scriptPubKey", {}).get("type")
        
        conn.execute(
            """
            INSERT INTO outputs (
                txid, vout_n, vout_value, vout_scriptPubKey_desc,
                vout_scriptPubKey_address, vout_scriptPubKey_type
            ) VALUES (?, ?, ?, ?, ?, ?)
            """, 
            (txid, vout_n, vout_value, vout_scriptPubKey_desc, vout_scriptPubKey_address, vout_scriptPubKey_type)
        )
        conn.commit()
        
def process_transactions(block, conn):
    for tx in block['tx']:
        txid = tx["txid"]
        # There is no cost feasible way to see when a Bitcoin transaction was created with certainty; these may be used as estimates though.
        median_blocktime = block.get("mediantime")
        miner_time = block.get("time")
        locktime = block.get("locktime")
        try:
            conn.execute(
                """
                INSERT INTO transactions (txid, median_blocktime, miner_time, locktime)
                VALUES (?, ?, ?)
                """, 
                (txid, median_blocktime, miner_time)
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
        try:
            get_vout(tx.get("vout", []), txid, conn)
            get_vin(tx.get("vin", []), txid, conn)
        except Exception as e:
            if "UNIQUE constraint failed" not in str(e):
                print(e)
                raise
            else:
                print(e)


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
                print(f"Processed block height: {height}")
            # Sleep to avoid overloading API
            sleep(0.1)  
            
    conn.close()
    
def main():
    start_height = int(input("Enter the starting block height: "))
    createBlockchainTxnDatabase(start_height)

if __name__ == "__main__":
    main()
