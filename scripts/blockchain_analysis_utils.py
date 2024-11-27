
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
    # Create wallet ID dictionary. The key is a one way hash function - this uniquely identifies this hash tree.
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
