import bitcoinlib
import pandas as pd
# The following modules can be used to link heterogenous transactions as belonging to a specific wallet in Bitcoin blockchain transaction data.
#######################################################################################################
# Creates a dictionary of derived addressed and compressed and uncompressed version(s) of the pubkey(s)
def deriveUndefinedAddresses(pubkey):
  # To do: Add exception to see if functions needed are imported
  # To do: Ensure pubkey is list
  # Determine if pubkey is compressed or uncompressed
  def deriveIndividualAddresses(ith_key):
    key = bitcoinlib.keys.Key(import_key = ith_key)
    if key.compressed == True:
      uncompressed_key = key.public_uncompressed_hex()
      compressed_key = key
    else:
      uncompressed_key = key
      compressed_key = key.public_compressed_hex()
      # To do: Add more address support.
    individualAddresses = {
        'Uncompressed_Public_Key': uncompressed_key,
        'Compressed_Public_Key': compressed_key,
        'Legacy_Address': uncompressed_key.address(encoding = 'base58', witness_type = 'legacy'),
        'Segwit_Address': compressed_key.address(encoding = 'base32', witness_type = 'segwit')
    }
    addressesDF = pd.DataFrame(individualAddresses)
    return addressesDF
  df_list = []
  for each in pubkey:
    df = deriveIndividualAddresses(each)
    df_list.append(df)
  merged_df = pd.concat(df_list, axis = 0, ignore_index = TRUE)
  return merged_df
####################################################################################################
# This function searches the dataset for any duplicated values in one of the columns created by the undefined address function (Uncompressed PK, Compressed PK, Legacy Address, Segwit Address)
# If any transactions share a duplicate and have missing values in any one of these observations, the missings will be overwritten by the non missing information in the other transactions.
# Finally, this function creates an "Identifier" column and appends a unique number identifying a transaction as belonging to the nth wallet. This is for ease of future search and potential data reduction.
# The purpose of this is to identify transactions belonging to a single wallet, as some wallets may use public keys at first then change to hashes.
# Dependency: Non-existent observations must be NaN - I will fix this later.
# Note: Warnings are caused by modifying the dataframe rather than the copies because of the use of .iloc() rather than .loc. Doesnt break anything and is irrelevant. Ill fix later.
def matchAddresses(txns, address_columns = ['Uncompressed_Public_Key', 'Compressed_Public_Key', 'Legacy_Address', 'Segwit_Address']):
  ## To do: Add support for non address scripts
  def findMatchedRows(row, df):
    matched_rows = df[df.apply(lambda j: any(value in row[address_columns].tolist() for value in j if not pd.isna(value)), axis=1)] # Is NA not working here for the Identifier column. Just subset for now, has to do with pd.NA
    return matched_rows
  addresses = txns[address_columns] # Subset only address columns for speed and to meet memory constraints.
  addresses['Identifier'] = pd.NA
  wallet_counter = 0
  # Find and fill matches for all fields with no NAs - reduces time complexity
  for i in range(len(addresses)):
    row = addresses.iloc[i]
    if any(pd.isna(address) for address in row[address_columns]): # Skip over all rows with non NAs for simplicity, will match them later.
      continue
    if not pd.isna(row['Identifier']):
      continue
    row['Identifier'] = wallet_counter
    matched_rows = findMatchedRows(row = row, df = addresses) # Possible speedup by using locf for NAs instead of copying
    matched_rows.loc[matched_rows.index] = row.values # Use subsetting to preserve index
    addresses.update(matched_rows)
    wallet_counter += 1
  # Find and fill matches for remaining with at least one NA
  for i in addresses[addresses['Identifier'].isna()].index:
    row = addresses.iloc[i]
    if not pd.isna(row['Identifier']): # Check if row has been filled
      continue
    matched_rows = findMatchedRows(row = row, df = addresses)
    for j in matched_rows[address_columns].columns:
      try:
        address = matched_rows[j].dropna().unique()[0]
      except:
        continue
      matched_rows[j].fillna(address, inplace = True)
    matched_rows['Identifier'] = wallet_counter
    addresses.update(matched_rows)
    wallet_counter += 1
  txns[address_columns] = addresses[address_columns]
  txns['Identifier'] = addresses['Identifier']
  return txns
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
