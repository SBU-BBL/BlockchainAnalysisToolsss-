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
            print('Around', offset, 'rows processed', sep = ' ')
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
            
            # Insert rows into the output_hashes table for each address
            for addr in parsed_result:
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

