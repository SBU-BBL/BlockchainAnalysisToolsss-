CREATE TABLE transactions (
    txid TEXT PRIMARY KEY,
    median_blocktime TIMESTAMP NULL,
    mempool_time TIMESTAMP NULL
);

CREATE TABLE outputs (
    indexing_id INTEGER PRIMARY KEY AUTOINCREMENT,
    txid TEXT NOT NULL,
    vout_value REAL,
    vout_n INTEGER,
    vout_scriptPubKey_asm TEXT,
    vout_scriptPubKey_desc TEXT,
    vout_scriptPubKey_hex TEXT,
    vout_scriptPubKey_address TEXT,
    vout_scriptPubKey_type TEXT,
    FOREIGN KEY (txid) REFERENCES transactions (txid)
);

CREATE TABLE inputs (
    indexing_id2 INTEGER PRIMARY KEY AUTOINCREMENT,
    txid TEXT NOT NULL,
    vin_txid TEXT,
    vin_vout INTEGER,
    FOREIGN KEY (txid) REFERENCES transactions (txid)
);

