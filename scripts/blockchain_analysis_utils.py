##########################################################################################################################################
##########################################################################################################################################
##############################################           Written by Noah Tover           #################################################
##########################################################################################################################################
##########################################################################################################################################


# This function creates a dictionary mapping different types of hashes to their highest level hash in the blockchain - most commonly public keys.
# This ensures we have base truth "clusters" before cluster heuristics.
def normalizeHashes(transactions_json, vout_asm="vout_scriptPubKey_asm", vout_address="vout_scriptPubKey_address", vout_type="vout_scriptPubKey_type"):
    df = pd.read_json(transactions_json)    
    df = df[[vout_asm, vout_address, vout_type]]
    # Explode the nested column lists for clean code and easy processing. 
    df = df.apply(pd.Series.explode, ignore_index=True)
    # Special handling for multisig, which has nested lists
    df.loc[df[vout_type] == 'multisig', vout_asm] = df.loc[df[vout_type] == 'multisig', vout_asm].apply(pd.Series.explode)
    df = df.applymap(str) # WARNING: Changes None values to "None" TODO: Fix this dependency
    # Drop duplicates based on the 'vout_scriptPubKey_asm' column. This also removes duplicate addresses.
    df = df.drop_duplicates(subset=vout_asm)
    # pubkey and multisig types are the highest standard hash levels.
    filtered_df = df.loc[df[vout_type].isin(['pubkey', 'multisig'])]
    
    # Parse public keys from asm
    publickeys = filtered_df[vout_asm].apply(parsePushData)
    # Derive several addresses from the public keys
    newly_defined_addresses = publickeys.apply(deriveUndefinedAddresses)
    
    # Create hash dictionary. The key is a one way hash function - this uniquely identifies this hash tree.
    hash_dictionary = {
        sha256(str(each).encode()).hexdigest(): each
        for each in newly_defined_addresses
    }
    # Add non public keys to the dictionary. Only using addresses as these are the only other standard scripts.  
        ## TODO: Add support for pay to script hash (for multisig addresses) - requires asm decoding. 
    df_pkhashes = df[vout_address][df[vout_address] != 'None'] # NOTE: Using str "None" because applymap changes None to strings :/
    for address in df_pkhashes:
        if any(address in value for value in hash_dictionary.values()):
            continue  # If the address is already mapped to an identifier, skip it
        else:
            hash_dictionary[address] = address  # Add the address to the dictionary

    return hash_dictionary

##########################################################################################################################################
# Merges a list of dictionaries with tuple keys and removes smaller tuples that are subsets of larger ones.
# Returns a dictionary containing only the largest unique tuples with their corresponding values.
# Can be used for consolidating clusters and normalized hashes calculated on each chunk of data.
def prune_dictionaries(list_of_dicts):
    # Merge all dictionaries into one
    merged_dict = {}
    for d in list_of_dicts:
        merged_dict.update(d)
    
    # Create a set of keys to remove.
    keys_to_remove = set()
    keys = list(merged_dict.keys())
    # Iterate through each key to check for smaller tuple subsets
    # TO DO: Change nested loop to recursion
    # TO DO: Avoid explicit looping through keys. Research to find a faster algorithm
    for i, key1 in enumerate(keys):
        for key2 in keys:
            if key1 != key2 and set(key1).issubset(set(key2)):  # If key1 is a subset of key2
                keys_to_remove.add(key1)  
    
    for key in keys_to_remove:
        if key in merged_dict:
            del merged_dict[key]
    
    return merged_dict
