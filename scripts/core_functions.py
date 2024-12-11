# Developed by Noah Tover #
import bitcoinlib
import pandas as pd
# The following modules can be used to link heterogenous transactions as belonging to a specific wallet in Bitcoin blockchain transaction data.
#######################################################################################################
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
    addresses = deriveIndividualAddresses(pubkey)
    address_list.append(addresses)
    
  return address_list
####################################################################################################
# This function gets the txid of an input, finds the output with a matching txid, and gets the identifying column, which can be implicitly linked to an address. TODO: Decide if getting addresses later.
# The function then returns a dataframe of the txid and the output addresses associated with it. This can easily be merged by the txid column to the original transaction dataset.
# Chunk size is the number of sequential blocks to search. If unspecified, it will search all blocks.
# TODO: Make it subset just the parts it needs with chunk_size
def matchInputOutput(txns, chunk_size, vin_txid_colname = 'vin_txid', vout_index_colname = 'vin_vout', txid_colname = 'txid', n_colname = 'vout_n'):
  txns['From_Identifier'] = pd.NA
  for i in range(len(txns)):
    row = txns.iloc[i]
    matched_rows = txns[txns[txid_colname] == row[vin_txid_colname]]
    matched_output = matched_rows[matched_rows[n_colname] == row['vout_index_colname']]
    txns.iloc[i]['From_Identifier'] = matched_output['Identifier'].iloc[0] # Problem: This input could be made from many outputs. How do i do this correctly?
  return txns
####################################################################################################
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
####################################################################################################
# Inputs a lists of lists with partially duplicated items and returns the largest list of unique values associated with each of those partial duplicates.
def unionFind(lists):
        parent = {}
        rank = {}
        
        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])  # Call recursively for speed! :P
            return parent[x]

        def union(x, y):
            rootX = find(x)
            rootY = find(y)
            if rootX != rootY:
                if rank[rootX] > rank[rootY]:
                    parent[rootY] = rootX
                elif rank[rootX] < rank[rootY]:
                    parent[rootX] = rootY
                else:
                    parent[rootY] = rootX
                    rank[rootX] += 1

        all_items = set(item for lst in lists for item in lst)
        for item in all_items:
            parent[item] = item
            rank[item] = 0

        for lst in lists:
            for i in range(len(lst) - 1):
                union(lst[i], lst[i + 1])

        #Assign items to the same group by root, this is the complete set of merged lists.
        groups = {}
        for item in all_items:
            root = find(item)
            if root not in groups:
                groups[root] = []
            groups[root].append(item)

        return list(groups.values())
####################################################################################################
import re

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

