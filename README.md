# Smart-Wallet-Tracking
The following is a library of functions useful for blockchain analysis with python 🐍 and optionally, SQL. This library has tools for hash normalization, heuristic clustering, developing cluster balances, and many more to come. I plan on posting my research results in the "notebooks" file to give a feel for how this library can be used for blockchain analysis. In the future, I plan on:

The psuedonymity of the Bitcoin blockchain may provide insight into the behaviors of successful traders. Although several methods exist to further anonymize one's transactions, many also exist to do the opposite. Techniques from blockchain forensics and analysis can potentially be used to develop signals and/or filters from a wallet cluster's trading activities. This project is an application of these techniques, with the end goal of identifying some edge hidden within the network. If Bitcoin trader's alpha can be reverse engineered from this public data, it suggests that there is significant alpha decay risk in the bitcoin markets. This would mean that Bitcoin trading necessiates anonymization - an additional fixed cost. This could potentially impact a Bitcoin strategies position on the risk curve.

### Using the Library:
Ensure all relevant packages are installed. All packages are available through pip install for Python 3.13. Anaconda has all but bitcoinlib. I'll later add a helper function to quickly install them all for you.

1.) Download the blockchain with a Bitcoin Core full node. To do this, you must set up your .conf file as specified in the documentation (if not already done), then navigate to the path where the daemon file is stored in the command prompt. Then, write “bitcoind” to start the bitcoin daemon.
2.) Create your relational database to store the transaction data. Fill out the arguments within the extract_bitcoin_data_V3.py file. While running your Bitcoin Daemon, open the command prompt and navigate to the path containing the “extract_bitcoin_data_V3.py” file. Run it with 
```
py extract_bitcoin_data_V3.py
```
Or
```
python extract_bitcoin_data_V3.py
```
You will be prompted for a starting height. This is the height the code will begin parsing transactions from, note it down so you can stop it and restart it later if needed.
Note: This step will take a while. To avoid corrupting your Bitcoin node, only use the “bitcoin-cli stop” command in the command prompt at the daemon file path and allow full shutdown before closing. You can use task manager for this purpose as well.
3.) Tidy up & simplify the data by running 
```
fillOutputHashes(db_path = "YOUR_PATH_HERE")
fillNormalizedHashes(db_path = "YOUR_PATH_HERE")
```
The former simply parses all public keys and stores them in the output_hashes table, as Bitcoin Core doesn't do this for you by default.
4.) Create initial clusters.
```
commonSpendCluster(db_path = "YOUR_PATH_HERE")
```
5. Functionality also exists for matching inputs to outputs (although this violates the 3rd normal form), parsing ASMs, and other various miscellaneous tasks.
### Definitions:
Normalized Hash:
A Bitcoin public key can be hashed in several ways. For example, pubkey1 -> legacyaddr1 & pubkey1 -> otheraddr1. Further, some transactions contain several public keys for a single output, such as multisigs. In linking transactions via heuristic clustering, it is first necessary to link public keys to all of their child nodes via a unique identifier. This is also known as a normalized hash.
Heuristic Cluster:
An educated guess as to which wallets belong to a specific entity. For example, common spend clustering assumes all input wallets in any transaction belong to the same owner, as they must control all the private keys to transact with these wallets simultaneously. 
Transaction:
The locking and unlocking of an unspent transaction output (UTXO) via a script. Most commonly, hashes of the private key are used. 
### Incoming Updates
1.) Neo4j support - just need to request admin permission for install. This will allow for things like rolling temporal clustering and significantly faster common spend clustering.
2.) Eigenvalue centrality and exchange cluster identification.
