##############################################
import os
import psycopg2
import bitcoinlib
import re
from time import sleep
from multiprocessing import Pool
from psycopg2.extras import execute_values
############################################
# Configure settings
DB_CONFIG = {
    "dbname": "postgres",
    "user": "NTOVER",
    "password": "bblpassword",
    "host": "localhost",
    "port": "5432"
}

CSV_DIR = r"D:\csv_dir"
delete_copied = True
signal_path = os.path.join(CSV_DIR, "done.signal") # This file is produced by extract_bitcoin_data when it finishes downloading csvs.
chunk_size_psqlwork = 65000
chunk_size_pythonwork = 25000
ncores = 8
############################################
# Dependencies..
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

def connect_db():
    """Establish a PostgreSQL connection. Hopefully this function makes it easier to switch libraries, if needed."""
    return psycopg2.connect(**DB_CONFIG)

def parseDesc(descriptor: str):
    descriptor = descriptor.replace(" ", "").split("#", 1)[0] if "#" in descriptor else descriptor
    key_pattern = re.compile(
        r'(\[[0-9A-Fa-f]{8}(?:/[0-9]+\'?)*\])?'  # Optional key origin
        r'('
        r'(?:xpub|xprv|tpub|tprv|[A-Za-z0-9]{4}pub)[A-Za-z0-9]+(?:/[0-9]+\'?)*(?:/\*)?\'?' # Extended keys 
        r'|0[2-3][0-9A-Fa-f]{64}'  # Compressed pubkey
        r'|04[0-9A-Fa-f]{128}'  # Uncompressed pubkey
        r'|[0-9A-Fa-f]{64}'  # X-only key or 64-char hex key
        r'|[1-9A-HJ-NP-Za-km-z]{50,52}'  # WIF key
        r')'
    )
    matches = key_pattern.findall(descriptor)
    return [key for (_, key) in matches]

def tuneDB_for_python_processing(cursor, conn):
    '''
    Gives the postgresql server less memory and cpu to prioritize in python processing.
    '''
    cursor.execute("SET max_parallel_workers = 4;") 
    cursor.execute("SET max_parallel_workers_per_gather = 2;")
    cursor.execute("SET maintenance_work_mem = '500MB';")
    cursor.execute("SET work_mem = '128MB'; ")
    conn.commit()
def tuneDB_for_psql_processing(cursor, conn):
    cursor.execute("SET max_parallel_workers = 8;") 
    cursor.execute("SET max_parallel_workers_per_gather = 4;")
    cursor.execute("SET maintenance_work_mem = '1GB';")
    cursor.execute("SET work_mem = '256MB'; ")
    conn.commit()


# -*- coding: utf-8 -*-
def copy_csvs_to_postgre():
    print("skipping :P")


        
def parsePubkeyDescriptors(chunk_size, commit_every = 1):
    '''
    Gets chunks of pubkey descriptors with null addresses and parses the public key into that address field. Parallel friendly.
    '''
    conn   = connect_db()         # one connection per worker
    cursor    = conn.cursor()
    counter  = 0                    # for commit batching
    while True:
        # 1) Grab the next slice of work, locking rows so no other worker sees them
        cursor.execute(
            """
            WITH cte AS (
                SELECT txid, vout_n, descriptor
                FROM   outputs
                WHERE  descriptor_type = 'pubkey'
                  AND (address IS NULL OR address = '')
                FOR UPDATE SKIP LOCKED
                LIMIT  %s
            )
            SELECT txid, vout_n, descriptor FROM cte;
            """,
            (chunk_size,)
        )
        rows = cursor.fetchall()
        if not rows:
            print("A worker finihed parsing pubkey descriptors into addresses. ")
            break   # Finished

        updates = []
        for txid, vout_n, descriptor in rows:
            try:
                pubkey = parseDesc(descriptor)[0]   # parseDesc returns a list for multisig support, as of now this only parses pubkeys though so only getting pubkey.
                updates.append((pubkey, txid, vout_n))
            except Exception as e:
                print(f"[worker {os.getpid()}] parse error for {txid}:{vout_n} – {e}")

        # 3) Bulk‑update the slice
        if updates:
            cursor.executemany(
                """
                UPDATE outputs
                   SET address = %s
                 WHERE txid   = %s
                   AND vout_n = %s;
                """,
                updates,
            )

        counter += 1
        if counter % commit_every == 0:
            conn.commit()     
            print("Parsed descriptor chunk commit to database")

    conn.commit()             # final flush
    cursor.close()
    conn.close()
def findRevealedPkeys():
    conn = connect_db()
    with conn.cursor() as cursor:
        print("Starting to look for revealed public keys...")
        cursor.execute("SET work_mem = '500MB';")
        cursor.execute("SET temp_buffers = '11GB';")
        cursor.execute("SET parallel_setup_cost = 0;")        
        cursor.execute("SET parallel_tuple_cost = 0;")
        
        cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM pg_proc
            WHERE proname = 'get_revealed_key'
              AND pg_catalog.pg_function_is_visible(oid)
        );
    """)
    function_exists = cursor.fetchone()[0]
    
    if not function_exists:
        cursor.execute("""
            CREATE OR REPLACE FUNCTION public.get_revealed_key(witness_data text, vin_asm text)
            RETURNS text
            LANGUAGE plpgsql
            IMMUTABLE PARALLEL SAFE
            AS $function$
            BEGIN
              IF witness_data IS NOT NULL THEN
                RETURN trim(
                         reverse(
                           split_part(
                             reverse(witness_data),
                             ',',
                             1
                           )
                         )
                       );
              ELSE
                RETURN trim(
                         reverse(
                           split_part(
                             reverse(vin_asm),
                             ' ',
                             1
                           )
                         )
                       );
              END IF;
            END;
            $function$;
        """)
        conn.commit()
        print("Function created.")

        cursor.execute("""
                       
        BEGIN;
        
        
        
        CREATE TEMP TABLE revealed_keys ON COMMIT DROP AS
        SELECT
          i.vin_txid   AS txid,
          i.vin_vout   AS vout_n,
          get_revealed_key(i.witness_data, i.vin_asm) AS revealed_key
        FROM inputs i
        JOIN outputs o
          ON o.txid = i.vin_txid
         AND o.vout_n = i.vin_vout
        WHERE o.descriptor_type = 'pubkeyhash'
          AND (i.witness_data IS NOT NULL OR i.vin_asm IS NOT NULL);
        
        CREATE INDEX ON revealed_keys (txid, vout_n);
        ANALYZE revealed_keys;
        
        -- 2.4: Single UPDATE
        UPDATE outputs o
        SET
          address         = r.revealed_key,
          descriptor_type = 'pubkey'
        FROM revealed_keys r
        WHERE o.txid   = r.txid
          AND o.vout_n = r.vout_n;
        
        COMMIT;

                       """)
        print("Found and replaced all revealed public keys.")
        cursor.execute("VACUUM ANALYZE;")
    conn.commit()
    
def fillNormalizedHashes(chunk_size, commit_every = 10):
    
    """
    This function fills in the normalized hashes table by mapping the lowest level hash to any hashes which can be derived from it. Parallel friendly.
    DEPENDENCY: deriveUndefinedAddresses()
    """
    conn = connect_db()
    conn.autocommit = False
    cursor = conn.cursor()
    counter = 0
    # Order for standardized chunks.
    sql = """
        SELECT address
        FROM   outputs
        WHERE  descriptor_type = 'pubkey'
          AND  NOT EXISTS (
                  SELECT 1
                  FROM   normalized_hashes nh
                  WHERE  nh.hash = outputs.address )
        ORDER  BY address
        FOR UPDATE SKIP LOCKED
        LIMIT  %s;
        """         
    while True:
        with conn.cursor() as cursor:
            cursor.execute(sql, (chunk_size, ))
            rows = cursor.fetchall()
            if not rows:
                conn.commit()
                cursor.close()
                conn.close()
                print("Normalization completed for a worker")
                return
            hash_rows = []
            for (pubkey,) in rows:
                try:
                    addrs = deriveUndefinedAddresses(
                        pubkey, assume_multisig_owned=True
                    )
                    root = addrs[0] # The root hash will just be a hash of all the other hashes. In the future this should probably just be a pubkey though for clarity.
                    
                    hash_rows.extend((h, root) for h in addrs[1:])
                    hash_rows.append((root, root)) # This is just to keep downstream dependencies working - the root hash used to be a composite hash but this made no sense.

                except Exception as e:
                    print(f"[{os.getpid()}] Error on {pubkey}: {e}")
                    
            execute_values(
                cursor,
                """
                INSERT INTO normalized_hashes (hash, root_hash)
                VALUES %s
                ON CONFLICT DO NOTHING;
                """,
                hash_rows,
            )
            counter += 1
            if counter % commit_every == 0:
                print("Batch commited")
                conn.commit()                 # release locks in batches

def trimDB():
    '''
    This makes the database smaller after populate_database is ran in full by reducing redundant information. 
    DEPENDENCY: Multisigs and all other types besides pubkey ignored as root_hash candidates.
    '''
    conn = connect_db()
    cursor = conn.cursor()
    try:
        # Set descriptor and descriptor_type to NULL in outputs where descriptor_type = 'pubkey'
        cursor.execute("""
            UPDATE outputs
            SET descriptor = NULL
            WHERE descriptor_type = 'pubkey';
        """)
        
        # Redundant because descriptors can be parsed for 
        cursor.execute("""
                       ALTER TABLE transactions
                       DROP COLUMN descriptor_type;
                       """)

        # Set witness_data and vin_asm to NULL in inputs where the referenced output has descriptor_type = 'pubkey'
        cursor.execute("""
            UPDATE inputs
            SET witness_data = NULL,
                vin_asm = NULL
            WHERE (vin_txid, vin_vout) IN (
                SELECT txid, vout_n
                FROM outputs
                WHERE descriptor_type IS NULL  -- Because we just nulled 'pubkey'
            );
        """)

        cursor.execute("VACUUM FULL ANALYZE;")

        conn.commit()
        print("Database trimmed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error occurred: {e}")
    finally:
        cursor.close()
        conn.close()

def commonSpendCluster():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SET temp_buffers = '10GB';") 
    cursor.execute("SET parallel_setup_cost = 0;")        
    cursor.execute("SET parallel_tuple_cost = 0;")
    sql = """
    
    DROP TABLE IF EXISTS tmp_input_nodes;
    CREATE UNLOGGED TABLE tmp_input_nodes AS
    SELECT DISTINCT
           i.vin_txid AS spent_txid,
           COALESCE(nh.root_hash, o.address, i.vin_txid || ':' || i.vin_vout) AS node_hash
    FROM inputs i
    JOIN outputs o ON o.txid = i.vin_txid AND o.vout_n = i.vin_vout
    LEFT JOIN normalized_hashes nh ON nh.hash = o.address
    WHERE i.vin_txid IS NOT NULL;
    
    CREATE INDEX ON tmp_input_nodes(node_hash);
    ANALYZE tmp_input_nodes;
    
    INSERT INTO normalized_hashes(hash, root_hash)
    SELECT ni.node_hash, ni.node_hash
    FROM tmp_input_nodes ni
    LEFT JOIN normalized_hashes nh ON nh.hash = ni.node_hash AND nh.root_hash = ni.node_hash
    WHERE nh.hash IS NULL
    ON CONFLICT DO NOTHING;
    
    DROP TABLE IF EXISTS tmp_spend_edges;
    CREATE UNLOGGED TABLE tmp_spend_edges AS
    SELECT DISTINCT
           LEAST(a.node_hash, b.node_hash) AS h1,
           GREATEST(a.node_hash, b.node_hash) AS h2
    FROM tmp_input_nodes a
    JOIN tmp_input_nodes b ON a.spent_txid = b.spent_txid AND a.node_hash <> b.node_hash;
    
    CREATE INDEX ON tmp_spend_edges(h1);
    CREATE INDEX ON tmp_spend_edges(h2);
    ANALYZE tmp_spend_edges;
    
    WITH RECURSIVE
        cc(node, comp) AS (
            SELECT node_hash, node_hash FROM (SELECT DISTINCT node_hash FROM tmp_input_nodes) n
            UNION ALL
            SELECT e.h1, LEAST(cc.comp, e.h2) FROM cc JOIN tmp_spend_edges e ON cc.node = e.h1 WHERE LEAST(cc.comp, e.h2) < cc.comp
            UNION ALL
            SELECT e.h2, LEAST(cc.comp, e.h1) FROM cc JOIN tmp_spend_edges e ON cc.node = e.h2 WHERE LEAST(cc.comp, e.h1) < cc.comp
        ),
        final_comp AS (
            SELECT node AS root_hash, MIN(comp) AS cluster
            FROM cc
            GROUP BY node
        )
    INSERT INTO cs_clusters(root_hash, cluster)
    SELECT root_hash, cluster
    FROM final_comp
    ON CONFLICT (root_hash, cluster) DO NOTHING;
    
    DROP TABLE IF EXISTS tmp_spend_edges;
    DROP TABLE IF EXISTS tmp_input_nodes;

    """
    print("Starting common spend graph discovery")
    cursor.execute(sql)
    print("Finished common spend graphy discovery!")
    

if __name__ == "__main__":
    copy_csvs_to_postgre()
    findRevealedPkeys()
    with Pool(ncores) as pool:
        print("Starting to parse descriptors...")
        pool.map(parsePubkeyDescriptors, [chunk_size_pythonwork] * ncores)
        pool.map(fillNormalizedHashes, [chunk_size_pythonwork] * ncores)
    trimDB()
    commonSpendCluster()
        
    print("Database population and normalization complete! :D")
    
