
                                                       Bitcoin Transaction PostgreSQL Schema
This schema is designed to store and analyze Bitcoin blockchain transaction data in a normalized and query-efficient format. The database tracks transaction inputs, outputs, public key information, and normalized address hashes for blockchain analysis analysis and clustering.

Schema Overview
transactions
Maps each transaction to information about time.

txid: Transaction ID (primary key).

median_blocktime: Median time from the block (UNIX time). Used for estimating time bounds (for intraday - this project isn't doing this yet).

miner_time: Actual mining time (UNIX format). (This is an estimate of the time a transaction occured)

locktime: Locktime value - this is the first blockheight at which an output can be unlocked.

outputs
Represents outputs of each transaction. Outputs can be thought of as the recipients of a transaction. In truth, outputs are the information required to lock the referenced input.

Composite primary key: (txid, vout_n).

vout_value: Output value (in BTC).

vout_scriptPubKey_desc: Descriptor string.

vout_scriptPubKey_address: Parsed address (if available).

vout_scriptPubKey_type: Output type (e.g., pubkey, P2PKH).

Foreign key: txid references transactions(txid).

NOTE: Time values are stored as BIGINT because postgresql doesn't allow UNIX timestamps.

output_hashes (currently unused)
A placeholder table to support analysis of address/script types, ignoring multisigs for now.

inputs
Represents inputs to transactions.

vin_txid, vin_vout: Reference the source output (nullable for coinbase txns).

vin_asm: ASM string of the unlocking script.

witness_data: Raw witness data for SegWit transactions.

normalized_hashes
Maps hashes derived from the highest level hashes (public keys) found in the blockchain. This is because one wallet can have multiple hashes. This standardizes a wallets hashes to a single hash- think of the root_hash as a wallets ID.
If a hash truly exists, it is in the vout_scriptPubKey_address column in outputs and should be treated as owned by root_hash.                                                 

hash: A hash that may or may not exist in outputs (vout_scriptPubKey_address). Derived from the highest level hash found in the blockchain (such as a public key or a multisig)

root_hash: The normalized version of the hash. Treat this as the ID.

Composite primary key: (hash, root_hash).

cs_clusters
Maps normalized root hashes to clusters (e.g., wallet groups).

root_hash: Foreign key to normalized_hashes(root_hash).

cluster: Assigned cluster ID.

Composite primary key: (root_hash, cluster).

Notes
output_hashes is currently ignored in analysis.

Foreign keys are selectively applied due to nullability (e.g., coinbase txns, inferred addresses).

Address parsing and clustering are handled by external processes and fed into normalized_hashes.
