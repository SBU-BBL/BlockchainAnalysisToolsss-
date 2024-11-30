##########################################################################################################################################
##########################################################################################################################################
##############################################           Written by Noah Tover           #################################################
##########################################################################################################################################
##########################################################################################################################################


# This function creates a dictionary mapping different types of hashes to their highest level hash in the blockchain - most commonly public keys.
# This ensures we have base truth "clusters" before cluster heuristics.
# Assumes data is stored in the expected database schema - see documentation folder
def normalizeHashes(transactions, vout_asm="vout_scriptPubKey_asm", vout_address="vout_scriptPubKey_address", vout_type="vout_scriptPubKey_type", ):
    df = transactions
    df = df[[vout_asm, vout_address, vout_type]]
    # Drop duplicates based on the 'vout_scriptPubKey_asm' column. This also removes duplicate addresses.
    df = df.drop_duplicates(subset=vout_asm)
    # pubkey and multisig types are the highest standard hash levels.
    filtered_df = df.loc[df[vout_type].isin(['pubkey', 'multisig'])]
    
    # Derive several addresses from the public keys
    newly_defined_addresses = publickeys.apply(deriveUndefinedAddresses)

    # Create a dictionary to map each hash to a unique ID
    hash_dictionary = {}    
    for each in newly_defined_addresses:
        # Hash the components of this tuple. 
        # This allows for homogenous unique IDs - hashes which cannot be mapped to any other hashes (their pubkey never is used in scripts) will be their own unique ID later on.
        unique_id = sha256(str(each).encode()).hexdigest()
        # Map each individual hash in the tuple to the same unique ID
        for hash_type in each:
            if hash_type not in hash_dictionary:
                hash_dictionary[hash_type] = unique_id    
                
    return hash_dictionary

##########################################################################################################################################
# Merges a list of dictionaries. Useful for normalizeHashes when several dictionaries are created from chunking. 
# Can be used for consolidating clusters and normalized hashes calculated on each chunk of data.
def prune_dictionaries(list_of_dicts):
    # Merge all dictionaries into one
    merged_dict = {}
    for each in dict_list:
        # All duplicate keys will have the same values (unique ids)
        merged_dict | d
    
    return merged_dict
