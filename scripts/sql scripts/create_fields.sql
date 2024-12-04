CREATE TABLE transactions (
    txid TEXT PRIMARY KEY,
    median_blocktime TIMESTAMP NULL,
    mempool_time TIMESTAMP NULL
);

CREATE TABLE outputs (
    txid TEXT NOT NULL,
    vout_n INTEGER NOT NULL,
    vout_value REAL,
    vout_scriptPubKey_asm TEXT,
    vout_scriptPubKey_desc TEXT,
    vout_scriptPubKey_hex TEXT,
    vout_scriptPubKey_address TEXT,
    vout_scriptPubKey_type TEXT,
    vout_wallet_ID varchar(256),
    PRIMARY KEY (txid, vout_n),
    FOREIGN KEY (txid) REFERENCES transactions (txid)
);


CREATE TABLE output_hashes (
    txid TEXT NOT NULL,
    vout_n INTEGER NOT NULL,
    address TEXT NOT NULL,
    PRIMARY KEY (txid, vout_n, address),
    FOREIGN KEY (txid, vout_n) REFERENCES outputs (txid, vout_n)
);

CREATE TABLE inputs (
    txid TEXT NOT NULL,
    vin_txid TEXT NULL,
    vin_vout INTEGER NULL,
    PRIMARY KEY (txid, vin_txid, vin_vout),
    FOREIGN KEY (vin_txid, vin_vout) REFERENCES outputs (txid, vout_n)
);

CREATE TABLE normalized_hashes (
    hash TEXT NOT NULL,
    root_hash TEXT NOT NULL,
    FOREIGN KEY (hash) REFERENCES output_hashes (address)
);

