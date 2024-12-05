
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
#########################################################################################################################################################################
# This function fills in the output hashes table by parsing the descriptor when necessary
def fillOutputHashes(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Grab chunks from outputs table
    batch_size = 5000  # Process rows in batches
    offset = 0
    
    while True:
        # Fetch a chunk of rows with well defined hashes
        # TODO: Add more supported types
        cursor.execute(f"""
            SELECT txid, vout_n, vout_scriptPubKey_address, vout_scriptPubKey_desc, vout_scriptPubKey_type
            FROM outputs
            WHERE vout_scriptPubKey_type IN ('pubkey', 'pubkeyhash', 'multisig')
            AND NOT EXISTS (
                SELECT 1
                FROM output_hashes
                WHERE outputs.txid = output_hashes.txid
                AND outputs.vout_n = output_hashes.vout_n
            )
            LIMIT {batch_size} OFFSET {offset};
        """)
        rows = cursor.fetchall()

        if not rows:
            print(offset, 'to', offset + batch_size, 'rows processed', sep = ' ')
            break  # Stop the loop when all rows have been processed.

        for row in rows:
            txid, vout_n, address, desc, scriptPubKey_type = row

            if address:
                # If address exists, add it
                addresses_to_use = [address]
            else:
                # Parse the descriptor to get the address or list of addresses
                parsed_result = parseDesc(desc)
                # Multisig descriptors have a list of keys, for this exception multiple rows should exist in output_hashes to conform to normal form.
                if isinstance(parsed_result, list) and scriptPubKey_type == "multisig":
                    addresses_to_use = parsed_result  # Multiple addresses for multisig
                else:
                    addresses_to_use = [parsed_result]  # Turn single address to list to make it generalizable.

            # Insert rows into the output_hashes table for each address
            for addr in addresses_to_use:
                cursor.execute("""
                    INSERT INTO output_hashes (txid, vout_n, address)
                    VALUES (?, ?, ?);
                """, (txid, vout_n, addr))

        # Commit updates after processing each batch
        conn.commit()
        offset += batch_size

    conn.close()

fillOutputHashes(db_path = r"E:\transactions_database")
########################################################################################################################
# TODO: Make these functions general.
def fillNormalizedHashes(db_path):
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    batch_size = 5000
    offset = 0

    while True:
        cursor.execute(f"""
            SELECT DISTINCT address
            FROM output_hashes
            WHERE type = 'pubkey'
            LIMIT {batch_size} OFFSET {offset};
        """)
        rows = cursor.fetchall()
        
        if not rows:
            break  

        normalized_data = normalizeHashes(rows)

        # Insert normalized data into normalized_hashes table
        for address, root_hash in normalized_data.items():
            cursor.execute("""
                INSERT INTO normalized_hashes (hash, root_hash)
                VALUES (?, ?);
            """, (address, root_hash))

        connection.commit()
        offset += batch_size

    connection.close()

