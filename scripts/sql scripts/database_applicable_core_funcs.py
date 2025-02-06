from hashlib import sha256
import bitcoinlib
import pandas as pd
import re

# Grabs the push data in between all the OP_ calls in the asm field, stores the results in a list
# This is intended to be used for parsing public keys from pubkey and multisig transcations.
# If individuals_in_list argument is set to False, single public keys will be returned as strings. By default, all returns are in lists.
def parsePushData(script_asm, individuals_in_list = True):
  # TODO: Add option for inputs.
  if not isinstance(script_asm, str):
    script_asm = str(script_asm)
  substrings = script_asm.split()
    # Subsets portion of hex before commands
  push_data = [substring for substring in substrings if not substring.startswith('OP')]
  if individuals_in_list == False:
    if len(push_data) == 1:
      push_data = push_data[0] # Unlist single public key
  return push_data
def parseDesc(descriptor: str):
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

def fillOutputHashes(db_path, chunk_size = 5000):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Grab chunks from outputs table
    batch_size = chunk_size  # Process rows in batches
    offset = 0
    print('Beginning hash parsing...')
    while True:
        # Fetch a chunk of rows with well defined hashes
        # TODO: Add more supported types
        query = f"""
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
        """
        df = pd.read_sql_query(query, conn)        
        if df.empty:
            print(f'Around {offset} rows processed')
            break
        
        df['vout_scriptPubKey_address'] = df.apply(
            lambda row: row['vout_scriptPubKey_address'] if row['vout_scriptPubKey_address'] else parseDesc(row['vout_scriptPubKey_desc']),
            axis=1
        )
        df = df.explode('vout_scriptPubKey_address') # Treat multisigs as separate rows
        
        df[['txid', 'vout_n', 'vout_scriptPubKey_address', 'vout_scriptPubKey_type']].to_sql(
        'output_hashes', conn, if_exists='append', index=False, 
        method='multi')

        # Commit updates after processing each batch
        print('Batch processed...')
        conn.commit()
        offset += batch_size

    conn.close()
########################################################################################################################
# TODO: Make these functions general.
# DEPENDENCIES: normalizeHashes function, import bitcoinlib, pandas as pd, deriveUndefinedAddresses function
def deriveUndefinedAddresses(pubkey, assume_multisig_owned = True, n_childkeys = 2):
  '''
  Creates a dictionary of derived addresses and compressed and uncompressed version(s) of the pubkey(s). This is a base truth hash tree.
  To be pedantic, these hashes are not "owned" by the controller of the public key (as multiple entities may share a public key), however they are controlled by them.
  If assume_multisig_owned is False, nested lists will be returned representing each public key's defined addresses so they can be treated separately.
  Assuming that a multisig "owns" all wallets it specifies is implicit common spend clustering, so it must be an exception. 
  n_childkeys specifies the number of childkeys to derive from an extended public key. There are a near infinite amount, so the default is small.
  DEPENDENCY: Multisig pubkeys should be passed in as lists, or individually.
  '''
  def is_extended_pubkey(k):
    # Check common prefixes of extended pubkeys. 
    # It is better practice to just see if an HDkey import fails, but because these keys make up so little of the data, I will only support these for speed.
    return k.startswith('xpub') or k.startswith('tpub') or k.startswith('ypub') or k.startswith('zpub') or k.startswith('vpub')
  # Determine if pubkey input is compressed or uncompressed, and then get the other version.
  def deriveIndividualAddresses(ith_key):
    key = bitcoinlib.keys.Key(import_key = ith_key)
    if key.compressed == True:
      uncompressed_key = bitcoinlib.keys.Key(import_key = key.public_uncompressed_hex)
      compressed_key = key
    else:
      uncompressed_key = key
      compressed_key = bitcoinlib.keys.Key(import_key = key.public_compressed_hex)
      # To do: Add more address support.
    legacy_address = uncompressed_key.address(encoding = 'base58', script_type = 'p2pkh')
    segwit_address = compressed_key.address(encoding = 'bech32', script_type = 'p2wpkh')
    defined_addresses = [key.public_uncompressed_hex, key.public_compressed_hex, legacy_address, segwit_address]
    return defined_addresses
  def derive_from_extended(xpub):
      xpub_defined_addresses = []
      hdkey = bitcoinlib.keys.HDKey(import_key=xpub)
      for i in range(n_childkeys):
          child_hdkey = hdkey.child(i)
          child_key_obj = child_hdkey.key()
          # Use the child's public key hex to derive addresses
          # The public key hex in compressed form can be obtained from child_key_obj.public_hex
          child_pub_hex = child_key_obj.public_hex
          xpub_defined_addresses = xpub_defined_addresses + deriveIndividualAddresses(child_pub_hex) + [child_pub_hex, child_key_obj.public_compressed_hex]
      return xpub_defined_addresses
  address_list = []
  if isinstance(pubkey, list):
    for each_key in pubkey:
        if is_extended_pubkey(each_key):
            # For an xpub, derive multiple child keys
            xpub_addresses = derive_from_extended(each_key)
            address_list.append(xpub_addresses)
        else:
            # Normal key, just derive addresses
            ithkey_addresses = deriveIndividualAddresses(each_key)
            address_list.append(ithkey_addresses)
    if assume_multisig_owned:
        # Flattens the list of lists.
        flattened = []
        for item in address_list:
            if isinstance(item, list):
                for sub in item:
                    flattened.extend(sub)
            else:
                flattened.extend(item)
        address_list = flattened
  else:
    address_list = deriveIndividualAddresses(pubkey)
    
  return address_list

def fillNormalizedHashes(db_path):
    def normalizeHashes(unique_pubkeys):
        # Derive all conventional address types from each public key. Assume all multisig public keys belong to the same wallet.
        # TODO: Write code to get unique pubkeys- multisig addresses contained in any other multisig address list are not unique
        newly_defined_addresses = [
            deriveUndefinedAddresses(pubkey, assume_multisig_owned=True)
            for pubkey in unique_pubkeys
        ]
        # Create a dictionary to map each hash to a unique ID
        hash_dictionary = {}    
        for each in newly_defined_addresses:
            # Hash the components of this tuple. 
            # This allows for homogenous unique IDs - hashes which cannot be mapped to any other hashes (their pubkey never is used in scripts) will be their own unique ID later on.
            unique_id = sha256(str(tuple(each)).encode()).hexdigest()
            # Map each individual hash in the tuple to the same unique ID
            for hash_type in each:
                if hash_type not in hash_dictionary:
                    hash_dictionary[hash_type] = unique_id    
                    
        return hash_dictionary

    connection = sqlite3.connect(db_path)

    batch_size = 5000
    offset = 0

    print('Beginning normalization...')
    while True:
        query = f"""
            SELECT DISTINCT vout_scriptPubKey_address
            FROM output_hashes
            WHERE vout_scriptPubKey_type = 'pubkey'
            LIMIT {batch_size} OFFSET {offset};
        """
        df = pd.read_sql_query(query, connection)

        if df.empty:
            print(f'Around {offset} rows processed')
            break
        
        normalized_data = normalizeHashes(df['vout_scriptPubKey_address'].tolist())
        
        normalized_df = pd.DataFrame(list(normalized_data.items()), columns=['hash', 'root_hash'])
        normalized_df.to_sql('normalized_hashes', connection, if_exists='append', index=False, method='multi')
        
        print('Batch processed...')
        connection.commit()
        offset += batch_size
    
    connection.close()
    
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
