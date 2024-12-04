##########################################################################################################################################
##########################################################################################################################################
##############################################           Written by Noah Tover           #################################################
##########################################################################################################################################
##########################################################################################################################################


# This function creates a dictionary mapping different types of hashes to their highest level hash in the blockchain - most commonly public keys.
# This ensures we have base truth "clusters" before cluster heuristics.
# Assumes data is stored in the expected database schema - see documentation folder
import hashlib
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
