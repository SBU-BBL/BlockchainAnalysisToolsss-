
import sqlite3

# This function parses the json of nested lists appropriately to tables within the database path. 
# This function is only for compatibility with other formats - preliminary block parsing can be done directly into the database.
def parseJsonToDB(data, db_path):
    with sqlite3.connect(db_path, timeout=30) as conn:
        cursor = conn.cursor()
    
        for record in data:
            try:
                
                median_blocktime = record.get('median_blocktime', None)
                mempool_time = record.get('mempool_time', None)
                cursor.execute("""
                    INSERT INTO transactions (txid, median_blocktime, mempool_time) 
                    VALUES (?, ?, ?)""", 
                    (record['txid'], median_blocktime, mempool_time)
                )
    
                # Insert into outputs
                vout_values = record.get('vout_value', [])
                vout_ns = record.get('vout_n', [])
                script_pubkey_asms = record.get('vout_scriptPubKey_asm', [])
                script_pubkey_descs = record.get('vout_scriptPubKey_desc', [])
                script_pubkey_addresses = record.get('vout_scriptPubKey_address', [])
                script_pubkey_types = record.get('vout_scriptPubKey_type', [])
    
                for index in range(len(vout_values)):
                    cursor.execute("""
                        INSERT INTO outputs (
                            txid, vout_value, vout_n, 
                            vout_scriptPubKey_asm, vout_scriptPubKey_desc,
                            vout_scriptPubKey_address, vout_scriptPubKey_type
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (
                            record['txid'], 
                            vout_values[index], 
                            vout_ns[index],
                            script_pubkey_asms[index], 
                            script_pubkey_descs[index],
                            script_pubkey_addresses[index], 
                            script_pubkey_types[index]
                        )
                    )
                vin_txid = record.get('vin_txid', None)
                vin_vout = record.get('vin_vout', None)
                if vin_vout is not None:
                    for index in range(len(vin_vout)):
                
                        cursor.execute("""
                            INSERT INTO inputs (txid, vin_txid, vin_vout)
                            VALUES (?, ?, ?)""",
                            (record['txid'], vin_txid[index], vin_vout[index])
                        )
                else:
                    cursor.execute("""
                        INSERT INTO inputs (txid, vin_txid, vin_vout)
                        VALUES (?, ?, ?)""",
                        (record['txid'], vin_txid, vin_vout)
                    )
                
            except Exception as e:
                print(f"Error processing record: {record['txid']}")
                print(f"Data causing error: {record}")
                print(f"Error details: {e}")
    
        # Commit all changes
        conn.commit()
    conn.close()
