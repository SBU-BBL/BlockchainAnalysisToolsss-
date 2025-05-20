CREATE TABLE transactions (
    txid INT PRIMARY KEY, -- Integer representation allows for fast indexing.
    txid_verbose BYTEA NULL, --Raw bytes of txid is stored here. 
    miner_time BIGINT NULL, -- Postgre doesn't allow unix in timestamps. This is for ease of import.
    locktime INT NULL
);

CREATE TABLE outputs (
    txid INT NOT NULL,
    vout_n INT NOT NULL,
    vout_value REAL,
    descriptor TEXT NULL,
    address TEXT NULL,
    descriptor_type BOOLEAN NULL, -- 1 if needs parsing, 2 if needs reveal, NULL if neither. This results in significant size compression + speedup.
    wallet_id INT NULL,
    PRIMARY KEY (txid, vout_n)
);

CREATE TABLE inputs (
    txid BYTEA NOT NULL, --Temporarily raw bytes prior to joining on transactions to get the txid integer. 
    vin_txid BYTEA NULL,
    vin_vout REAL NULL,
    asm_redeem TEXT NULL, --Only contains the last item to save significant space. This is all thats needed for revealed public keys. 
    witness_redeem TEXT NULL -- Same as above.
);

CREATE TABLE normalized_hashes (
    hash TEXT NOT NULL,
    wallet_id INT NOT NULL,
);

CREATE TABLE cs_clusters (
    wallet_id INT NOT NULL,
    cluster_id INT NOT NULL,    
);
