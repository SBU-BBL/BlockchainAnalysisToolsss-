CREATE TABLE transactions (
    txid TEXT PRIMARY KEY,
    median_blocktime TIMESTAMP NULL,
    miner_time TIMESTAMP NULL,
    locktime TEXT NULL
);

CREATE TABLE outputs (
    txid TEXT NOT NULL,
    vout_n INTEGER NOT NULL,
    vout_value REAL,
    vout_scriptPubKey_desc TEXT,
    vout_scriptPubKey_address TEXT,
    vout_scriptPubKey_type TEXT,
    vout_wallet_ID varchar(256),
    PRIMARY KEY (txid, vout_n),
    FOREIGN KEY (txid) REFERENCES transactions (txid)
);


CREATE TABLE output_hashes (
    txid TEXT NOT NULL,
    vout_n INTEGER NOT NULL,
    vout_scriptPubKey_address TEXT NOT NULL,
    vout_scriptPubKey_type TEXT NOT NULL,
    PRIMARY KEY (txid, vout_n, vout_scriptPubKey_address),
    FOREIGN KEY (txid, vout_n) REFERENCES outputs (txid, vout_n)
);

CREATE TABLE inputs (
    txid TEXT NOT NULL,
    vin_txid TEXT NULL,
    vin_vout INTEGER NULL,
    vin_wallet_ID TEXT,
    vin_asm TEXT NULL,
    witness_data TEXT NULL,
    PRIMARY KEY (txid, vin_txid, vin_vout),
    FOREIGN KEY (vin_txid, vin_vout) REFERENCES outputs (txid, vout_n)
);

CREATE TABLE normalized_hashes (
    hash TEXT NOT NULL,
    root_hash TEXT NOT NULL
);

CREATE TABLE cs_clusters (
	root_hash TEXT NOT NULL,
	cluster TEXT NOT NULL,
	FOREIGN KEY (root_hash) REFERENCES normalized_hashes (root_hash)
);

CREATE TABLE temporal_clusters (
	root_hash TEXT NOT NULL,
	cluster TEXT NOT NULL,
	FOREIGN KEY (root_hash) REFERENCES normalized_hashes (root_hash)
);
