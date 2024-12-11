# This function fills in the output hashes table by parsing the descriptor when necessary
import re
def fillOutputHashes(db_path):
    def parseDesc(descriptor: str):
      '''
      Function capable of parsing descriptors with explicity defined public keys. Capable of dealing with nested descriptors. Can parse script, tree, and key expressions.
      '''
        # Remove whitespace
        descriptor = descriptor.replace(" ", "")
        # Remove checksum information
        if "#" in descriptor:
            descriptor = descriptor.split("#", 1)[0]
    
        # re expressions to classify public key(s) based on script, tree, and key expressions. 
        # Subsets key - all irrelevant brackets and whatnot are ignored, ensuring only the relevant key is returned.
        # Source: "Support for Output Descriptors in Bitcoin Core" https://github.com/bitcoin/bitcoin/blob/master/doc/descriptors.md
    
        key_pattern = re.compile(
            r'(\[[0-9A-Fa-f]{8}(?:/[0-9]+\'?)*\])?'    # Optional key origin
            r'('
            r'(?:xpub|xprv|tpub|tprv|[A-Za-z0-9]{4}pub)[A-Za-z0-9]+(?:/[0-9]+\'?)*(?:/\*)?\'?' # Extended keys 
            r'|0[2-3][0-9A-Fa-f]{64}'      # Compressed pubkey
            r'|04[0-9A-Fa-f]{128}'         # Uncompressed pubkey
            r'|[0-9A-Fa-f]{64}'            # X-only key or 64-char hex key
            r'|[1-9A-HJ-NP-Za-km-z]{50,52}' # WIF key
            r')'
        )
    
        matches = key_pattern.findall(descriptor)
        # Returns a list. Some descriptors contain multiple public keys.
        keys = [key for (origin, key) in matches]
        return keys

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
# DEPENDENCIES: normalizeHashes function, import bitcoinlib, pandas as pd, deriveUndefinedAddresses function
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
########################################################################################################################
import sqlite3
import shelve
import os

def commonSpendCluster(database_path, visited_shelve_path=None, print_every=40, batch_size=10000):
    """
     Perform common spend clustering to group vin_wallet_IDs that appear together in transaction inputs via depth first search.

    Arguments:
    - database_path (str): Path to the SQLite database file.
    - visited_shelve_path (str): Path to the shelve database used for storing visited nodes and cluster IDs, if it exists. Otherwise one will be created.
    - print_every (int): How often to print how many clusters have been processed.
    - batch_size (int): The size of batches to process at once.
    Returns:
    - Adds a table to the database called "csc_clusters". The primary (foreign) key being the wallet_ID, the other value being the cluster to which it is assigned.
    NOTE: This code is extremely slow. Neo4j is a more appropriate way of doing this, but to expedite things (ironically) I wrote this code so we can do it w/o admin priviledges.
    """

    def get_neighbors(vin_wallet_ID, conn):
        """Retrieve neighboring vin_wallet_IDs that share transactions with the given vin_wallet_ID."""
        neighbors = []
        neighbor_offset = 0
        while True:
            cursor = conn.cursor()
            cursor.execute(f'''
            SELECT DISTINCT i2.vin_wallet_ID
            FROM inputs i1
            JOIN inputs i2 ON i1.txid = i2.txid
            WHERE i1.vin_wallet_ID = ?
              AND i2.vin_wallet_ID IS NOT NULL
              AND i2.vin_wallet_ID != i1.vin_wallet_ID
            LIMIT {batch_size} OFFSET {neighbor_offset}
            ''', (vin_wallet_ID,))
            rows = cursor.fetchall()
            if not rows:
                break
            for row in rows:
                neighbors.append(row[0])
            neighbor_offset += batch_size
        return neighbors

    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Create or open the visited shelve database
    if visited_shelve_path is None:
        visited_shelve_path = 'visited_shelve.db'
    visited_db = shelve.open(visited_shelve_path)

    # Create the csc_clusters table if it doesn't exist
    # Assuming output_hashes table exists with a wallet_ID primary key
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS csc_clusters (
        wallet_ID TEXT PRIMARY KEY,
        cluster_id INTEGER,
        FOREIGN KEY(wallet_ID) REFERENCES output_hashes(wallet_ID)
    )
    ''')
    conn.commit()

    cluster_id = 0  # Initialize cluster ID
    processed_count = 0

    print("Starting depth first search to find clusters...")

    # Retrieve all unique vin_wallet_IDs in batches using LIMIT/OFFSET
    offset = 0
    while True:
        cursor.execute(f'''
        SELECT DISTINCT vin_wallet_ID
        FROM inputs
        WHERE vin_wallet_ID IS NOT NULL
        LIMIT {batch_size} OFFSET {offset}
        ''')
        rows = cursor.fetchall()
        if not rows:
            break
        # Implement depth first
        for (vin_wallet_ID,) in rows:
            if vin_wallet_ID not in visited_db:
                cluster_id += 1  
                stack = [vin_wallet_ID]

                while stack:
                    current_vin_wallet_ID = stack.pop()
                    if current_vin_wallet_ID not in visited_db:
                        visited_db[current_vin_wallet_ID] = cluster_id

                        neighbors = get_neighbors(current_vin_wallet_ID, conn)

                        # Add unvisited neighbors to the stack
                        for neighbor in neighbors:
                            if neighbor not in visited_db:
                                stack.append(neighbor)

                processed_count += 1
                if processed_count % print_every == 0:
                    print(f"Processed {processed_count} clusters...")

        offset += batch_size

    print("DFS traversal complete.")

    # Insert the cluster results into the csc_clusters table
    visited_db.close()
    visited_db = shelve.open(visited_shelve_path, flag='r')

    insert_batch = []
    for vin_wallet_ID, cid in visited_db.items():
        insert_batch.append((vin_wallet_ID, cid))
        if len(insert_batch) >= batch_size:
            cursor.executemany('INSERT OR REPLACE INTO csc_clusters (wallet_ID, cluster_id) VALUES (?,?)', insert_batch)
            conn.commit()
            insert_batch.clear()

    # Insert any remaining records
    if insert_batch:
        cursor.executemany('INSERT OR REPLACE INTO csc_clusters (wallet_ID, cluster_id) VALUES (?,?)', insert_batch)
        conn.commit()

    visited_db.close()
    conn.close()

    # Get rid of all the temp files we used! Woohoo
    for filename in [visited_shelve_path, visited_shelve_path + '.db', visited_shelve_path + '.dat', visited_shelve_path + '.dir', visited_shelve_path + '.bak']:
        if os.path.exists(filename):
            os.remove(filename)

    print("Common spend clustering completed and added to database! :D")
    return 

    connection.close()

