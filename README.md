### Smart-Wallet-Tracking ###
The following is a library of functions useful for blockchain analysis with python 🐍 and SQL. This library has tools for hash normalization, heuristic clustering, developing cluster balances, and many more to come. I plan on posting my research results in the "notebooks" file to give a feel for how this library can be used for blockchain analysis. 

The psuedonymity of the Bitcoin blockchain may provide insight into the behaviors of successful traders. Although several methods exist to further anonymize one's transactions, many also exist to do the opposite. Techniques from blockchain forensics and analysis can potentially be used to develop signals and/or filters from a wallet cluster's trading activities. This project is an application of these techniques, with the end goal of identifying some edge hidden within the network. If Bitcoin trader's alpha can be reverse engineered from this public data, it suggests that there is significant alpha decay risk in the bitcoin markets. This would mean that Bitcoin trading necessiates anonymization - an additional fixed cost. This could potentially impact a Bitcoin strategies position on the risk curve.
### Dependencies ###
- psycopg2
- pandas
- re
- hashlib
- multiprocessing
- threading
### Using the Library ###
1.) Download the blockchain with a Bitcoin Core full node and populate a postgreSQL database with that data using extract_bitcoin_data_beta.py. Please follow the steps in the extract_bitcoin_data_beta README to do so. 
Run it in the command prompt with:
```
py extract_bitcoin_data_V3.py
```
Or
```
python extract_bitcoin_data_V3.py
```
You will be prompted for a starting height. This is the height the code will begin parsing transactions from, note it down so you can stop it and restart it later if needed.
Note: This step will take a while. To avoid corrupting your Bitcoin node, only use the “bitcoin-cli stop” command in the command prompt at the daemon file path and allow full shutdown before closing. You can use task manager for this purpose as well.
2.) Transform the data into an easy to analyze format using: 
```
fillParsedHashTables.py
```
This function essentially finds the highest level hash, if one is available, and maps it to all of its descendant nodes. This is important because a hash can produce many sub hashes. For example, a single public key can produce a traditional address, segwit address, etc.. Furthermore, this function normalizes the database for multisig addresses through the multi_output_hashes table. 
4.) Create initial clusters. Common spend clustering is the process of finding weakly connected components in input transactions. The latter is faster as neo4j is specifically built for graph applications.
```
commonSpendCluster(db_path = "YOUR_PATH_HERE")
```
OR
```
neo4jcommonSpendCluster(db_path = "YOUR_PATH_HERE")
```
5.) Calculate cluster balances through time
### Definitions:
Normalized Hash:
A Bitcoin public key can be hashed in several ways. For example, pubkey1 -> legacyaddr1 & pubkey1 -> otheraddr1. Further, some transactions contain several public keys for a single output, such as multisigs. In linking transactions via heuristic clustering, it is first necessary to link public keys to all of their child nodes via a unique identifier. This is also known as a normalized hash.
Heuristic Cluster:
An educated guess as to which wallets belong to a specific entity. For example, common spend clustering assumes all input wallets in any transaction belong to the same owner, as they must control all the private keys to transact with these wallets simultaneously. 
Transaction:
The locking and unlocking of an unspent transaction output (UTXO) via a script. Most commonly, hashes of the private key are used. 
### Incoming Updates
1.) Eigenvalue centrality and exchange cluster identification.
2.) Subsetting market predictive wallets NOT clustered who interact with exchanges (likely non anonymizing traders).

