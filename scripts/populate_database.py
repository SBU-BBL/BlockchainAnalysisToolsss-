import os
import psycopg2
import threading
from hashlib import sha256
import bitcoinlib
import re
import pandas as pd
from time import sleep
from math import ceil

# Temporary CSV storage
DB_CONFIG = {
    "dbname": "postgres",
    "user": "NTOVER",
    "password": "bblpassword",
    "host": "localhost",
    "port": "5432"
}

# Directory containing CSV files
CSV_DIR = r"D:\csv_dir"
delete_copied = True
signal_path = os.path.join(CSV_DIR, "done.signal") # This file is produced by extract_bitcoin_data when it finishes downloading csvs.



def backup_data(drive_x, drive_y, file_x, file_y):
    """
    This function backs data up to google drive. If data_dir is a database, it will pause any of the functions in this library that write to the database.
    Honestly this is the sloppiest and nastiest function i have written in my entire life. Developing without permissions is hard :))))))
    Dependencies:
        Google drive is open in browser at 500, 500
    Arguments:
        drive_x, drive_y, coordinates of the google drive browser open.
        file_x, file_y, coordinates of the file to backup.
    """
    import pyautogui
    import time
    pauser = pauserfunc()
    pauser.pause()
    pauser.wait_for_all_to_pause()

    
    # Drag and drop the file
    pyautogui.moveTo(file_x, file_y)
    pyautogui.mouseDown()  # Click and hold
    pyautogui.moveTo(drive_x, drive_y, duration=1)  
    pyautogui.mouseUp()  # Release the file
    
    print("File uploaded successfully! Now waiting 15 hours. Ugly software causes ugly fixes sorry lol")
    time.sleep(8 * 60 * 60)
    print("File upload likely complete! Unpausing..")
    pauser.resume()





class pauserfunc:
    """
    Pauses any of this library's function's chunked SQL writes.
    Directions:
        Specify a pauser = pauserfunc() inside each function that will use this. At each point where its okay to pause, put a pauser.wait_to_resume.
    Methods:
        .pause -> Pauses 
        .resume -> Resumes
        .wait_to_resume -> Checkpoints for where a function will wait while paused.
        .wait_for_all_to_pause -> Waiting area while a function that needs all paused waits for them to finish their transaction.
    """
    _instance = None  

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(pauserfunc, cls).__new__(cls)
            cls._instance.pause_event = threading.Event()
            cls._instance.pause_event.set()  
            cls._instance.lock = threading.Lock()  # To safely update active counter
            cls._instance.condition = threading.Condition(cls._instance.lock)  # Notify when all are paused
            cls._instance.active_count = 0  # Number of active functions
        return cls._instance
    
    def pause(self):
        with self.lock:
            self.pause_event.clear()  

    def resume(self):
        with self.lock:
            self.pause_event.set()  
            self.condition.notify_all() 
    
    def wait_to_resume(self):
        with self.lock:
            self.active_count += 1 
            self.condition.notify_all()  
        
        self.pause_event.wait()  
        
        with self.lock:
            self.active_count -= 1  
            self.condition.notify_all()  

    def wait_for_all_to_pause(self):
        with self.lock:
            while self.active_count > 0:  
                self.condition.wait()
                
def copy_csvs_to_postgre(all_csvs_downloaded = False):
    """
    Process all CSV files in the specified folder to their respective table in the postgre database. Supports continuously growing folders.
    PARAMETERS:
        - all_csvs_downloaded = True if the csv list is already comprehensive and not being continuously updated by extract_bitcoin_data script.
    DEPENDENCIES:
        - csvs must be named like tablename...csv
        - csvs must have a matching format to the table names of the postgresql database.
    
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    pauser = pauserfunc()
    cursor.execute("SET maintenance_work_mem = '1.9GB';")
    conn.commit()
    errors = [] # Track any errors that occur.

    try:
        # This while loop just rechecks the folder to see if any new files have been added after all of the initially found are processed.
        while True:
            folder_contents = os.listdir(CSV_DIR) 

            if not folder_contents and (os.path.exists(signal_path) or all_csvs_downloaded): 
                print("All csvs populated into database.")
                break 
            elif not folder_contents:
                sleep(600) # Wait for csvs to get downloaded..
                
            
            for filename in folder_contents:
                
                if filename.endswith(".csv"):
                    filepath = os.path.join(CSV_DIR, filename)
                    table_name = filename.split(sep = "_")[0] # Assuming format like tablename_....csv
                    
                    try:
                        with conn.cursor() as cursor:
                            with open(filepath, 'r') as f:
                                cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
                            conn.commit()
                            print(f"Successfully loaded {filepath} into {table_name}")
                    
                        if delete_copied == True:
                            # Delete CSV after successful upload (storage issue)
                            os.remove(filepath)  
                            print(f"Deleted {filepath}")
                            
                            pauser.wait_to_resume() # Pause checkpoint.
                    except Exception as e:
                        print(f"Error processing {filename}: {e}")
                        errors.append(e)
                        conn.rollback() # This resets the failed transaction state and allows other files to be downloaded to the database.
                        continue



    finally:
        conn.close()
        print("All CSVs copied into database!")
        return errors
        
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


chunk_size = 3000  # Adjust as needed

#######################################
def connect_db():
    """Establish a PostgreSQL connection. Hopefully this function makes it easier to switch libraries, if needed."""
    return psycopg2.connect(**DB_CONFIG)

def parsePushData(script_asm, individuals_in_list=True):
    if not isinstance(script_asm, str):
        script_asm = str(script_asm)
    substrings = script_asm.split()
    push_data = [substring for substring in substrings if not substring.startswith('OP')]
    if not individuals_in_list and len(push_data) == 1:
        push_data = push_data[0]
    return push_data

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

def fillOutputHashes(chunk_size=5000, delete_desc = False):
    """Fills or replaces the address field with public keys from descriptors or revealed public keys in the inputs."""
    conn = connect_db()
    cursor = conn.cursor()
    
    print("Indexing columns for fast lookups")
    index_queries = [
    # Primary keys
    "ALTER TABLE transactions ADD PRIMARY KEY (txid);",
    "ALTER TABLE outputs ADD PRIMARY KEY (txid, vout_n);",
    "CREATE INDEX IF NOT EXISTS idx_inputs_vin ON inputs(vin_txid, vin_vout);",
    "CREATE INDEX IF NOT EXISTS idx_outputs_type ON outputs(vout_scriptPubKey_type);",
    "CREATE INDEX IF NOT EXISTS idx_outputs_address ON outputs(vout_scriptPubKey_address);",
    "ALTER TABLE outputs ADD CONSTRAINT fk_outputs_txid FOREIGN KEY (txid) REFERENCES transactions (txid);",
    "ALTER TABLE inputs ADD CONSTRAINT fk_inputs_outputs FOREIGN KEY (vin_txid, vin_vout) REFERENCES outputs (txid, vout_n);"
    ]

    for query in index_queries:
        try:
            cursor.execute(query)
            print(f"Executed: {query}")
        except Exception as e:
            print(f"Error with query: {query}\n{e}")
    
    conn.commit()
        

    print('Beginning hash parsing...')
    
    revealedpk_query = """
    WITH revealedkeys AS (
        SELECT 
            i.vin_txid AS referenced_txid,
            i.vin_vout AS referenced_vout_n,
            CASE 
                WHEN position(' ' IN i.vin_asm) > 0 
                THEN substring(i.vin_asm FROM position(' ' IN i.vin_asm) + 1)
                ELSE NULL
            END AS extracted_address
        FROM inputs i
        JOIN outputs o ON i.vin_txid = o.txid AND i.vin_vout = o.vout_n
        WHERE o.vout_scriptPubKey_type = 'pubkeyhash'
        LIMIT %s
    )
    UPDATE outputs
    SET vout_scriptPubKey_address = revealedkeys.extracted_address,
        vout_scriptPubKey_type = 'pubkey'
    FROM revealedkeys
    WHERE outputs.txid = revealedkeys.referenced_txid
    AND outputs.vout_n = revealedkeys.referenced_vout_n;
    """
    
    while True:  
        cursor.execute(revealedpk_query, (chunk_size,))
        conn.commit()
        
        if cursor.rowcount == 0:
            print("Addresses replaced with revealed classic public keys")
            break

    witness_query = """
    SELECT i.vin_txid, i.vin_vout, i.witness_data
    FROM inputs i
    JOIN outputs o ON i.vin_txid = o.txid AND i.vin_vout = o.vout_n
    WHERE o.vout_scriptPubKey_type = 'witness_v0_keyhash'
    AND i.witness_data IS NOT NULL
    LIMIT %s;
    """
    
    while True:
        cursor.execute(witness_query, (chunk_size,))
        rows = cursor.fetchall()
        if not rows:
            break

        updates = [(witness.split(", ")[-1], txid, vout_n) for txid, vout_n, witness in rows]
        update_query = """
        UPDATE outputs
        SET vout_scriptPubKey_address = %s, vout_scriptPubKey_type = 'pubkey'
        WHERE txid = %s AND vout_n = %s;
        """
        cursor.executemany(update_query, updates)
        conn.commit()
    # Delete the descriptor column to save space.
    if delete_desc == True:
        cursor.execute("""
                       ALTER TABLE outputs DROP COLUMN vout_scriptPubKey_desc;
                       """)
    conn.close()

# Honestly I dont remember why I am not just using COPY instead of bulk inserts. Probably because COPY is all or nothing.
def append_table(df, table_name, conn):
    """Batch-inserts a dataframe into PostgreSQL."""
    if df.empty:
        return
    
    cursor = conn.cursor()
    columns = list(df.columns)
    placeholders = ", ".join(["%s"] * len(columns))
    cols_str = ", ".join(columns)
    sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    try:
        cursor.executemany(sql, data)
        conn.commit()
    except Exception as e:
        print(f"Batch insert failed: {e}")
        raise
        conn.commit()

def fillNormalizedHashes(chunk_size=5000):
    """Normalizes hashes and stores them in the normalized_hashes table."""
    conn = connect_db()
    cursor = conn.cursor()

    def normalizeHashes(unique_pubkeys):
        """Derives all conventional address types from each public key."""
        newly_defined_addresses = [
            deriveUndefinedAddresses(pubkey, assume_multisig_owned=True)
            for pubkey in unique_pubkeys
        ]
        hash_dictionary = {}    
        for each in newly_defined_addresses:
            unique_id = sha256(str(tuple(each)).encode()).hexdigest()
            for hash_type in each:
                if hash_type not in hash_dictionary:
                    hash_dictionary[hash_type] = unique_id    
        return hash_dictionary

    offset = 0
    print('Beginning normalization...')
    while True:
        query = f"""
            SELECT DISTINCT vout_scriptPubKey_address
            FROM outputs
            WHERE vout_scriptPubKey_type = 'pubkey'
            LIMIT {chunk_size} OFFSET {offset};
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print(f'Around {offset} rows processed')
            break

        normalized_data = normalizeHashes(unique_pubkeys=[row[0] for row in rows])
        normalized_df = pd.DataFrame(list(normalized_data.items()), columns=['hash', 'root_hash'])
        append_table(df=normalized_df, table_name="normalized_hashes", conn=conn)

        offset += chunk_size

    conn.close()


def main():
    print("Starting...")
    any_download_errors = copy_csvs_to_postgre() # This naming is slightly unclear, but copy_csvs_to_postgre returns a list of errors if any happened so the following two functions do not execute.
    if not any_download_errors:
        fillOutputHashes()
        fillNormalizedHashes()
    
if __name__ == "__main__":
    main()


