
import ijson
import sqlite3
# This function imports bitcoin JSONs into the tables of the supported database schema. 
def parse_json_to_db(file_path, db_path):
    with sqlite3.connect(db_path, timeout=30) as conn:
        cursor = conn.cursor()

        # Stream parsing the JSON file
        with open(file_path, 'r') as file:
            parser = ijson.items(file, 'item') # Use ijson to support larger than memory jsons.
            for record in parser:
                try:
                    median_blocktime = record.get('median_blocktime', None)
                    mempool_time = record.get('mempool_time', None)
                    cursor.execute("""
                        INSERT INTO transactions (txid, median_blocktime, mempool_time) 
                        VALUES (?, ?, ?)""", 
                        (record['txid'], median_blocktime, mempool_time)
                    )

                    vout_values = record.get('vout_value', [])
                    vout_values = [float(value) for value in vout_values] # Change to float for SQL table support.
                    vout_ns = record.get('vout_n', [])
                    script_pubkey_asms = record.get('vout_scriptPubKey_asm', [])
                    script_pubkey_descs = record.get('vout_scriptPubKey_desc', [])
                    script_pubkey_addresses = record.get('vout_scriptPubKey_address', [])
                    script_pubkey_types = record.get('vout_scriptPubKey_type', [])
                    # Insert each respective item in the list as its own row, effectively exploding the JSON. This fits the normal form.
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

                    # Insert each respective item in the list of inputs as its own row, effecitvely exploding the JSON. This fits the normal form.
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
                # Print exceptions to catch errors.
                except Exception as e:
                    print(f"Error processing record: {record['txid']}")
                    print(f"Data causing error: {record}")
                    print(f"Error details: {e}")

        # Commit changes at the end
        conn.commit()
    conn.close()
