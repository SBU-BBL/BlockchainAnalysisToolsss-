CREATE TABLE transactions (
    txid TEXT PRIMARY KEY,
    median_blocktime BIGINT NULL,
    miner_time BIGINT NULL, -- Postgre doesn't allow unix in timestamps. This is for ease of import.
    locktime TEXT NULL
);

CREATE TABLE outputs (
    txid TEXT NOT NULL,
    vout_n REAL NOT NULL,
    vout_value REAL,
    descriptor text NULL,
    address text NULL,
    descriptor_type text,
    vout_wallet_ID text NULL,
    PRIMARY KEY (txid, vout_n),
    FOREIGN KEY (txid) REFERENCES transactions (txid)
);

CREATE INDEX idx_outputs_type ON outputs(descriptor_type); -- Pubkey type transactions are regularly subset
CREATE INDEX idx_outputs_address ON outputs(address); -- Addresses are usually referenced as well.

-- (vin_txid, vin_vout) will be indexed after processing. (vin_txid, vin_vout) contains nulls because of coinbase transactions - but non nulls reference outputs (txid, vout_n)
CREATE TABLE inputs (
    txid TEXT NULL,
    vin_txid TEXT NULL,
    vin_vout REAL NULL,
    vin_asm TEXT NULL,
    witness_data TEXT NULL
);

CREATE INDEX idx_inputs_vin ON inputs(vin_txid, vin_vout); -- In the future, this should be a primary key for non coinbase transactions. Full normalization should create a separate table linked to inputs by a foreign key of input_type.

-- Please keep in mind that the hash may or may not exist in the address column as it is derived. Thus, a foreign key isnt used (this is a bandaid)
CREATE TABLE normalized_hashes (
    hash TEXT NOT NULL,
    root_hash TEXT NOT NULL,
    PRIMARY KEY (hash, root_hash)
);

CREATE TABLE cs_clusters (
    root_hash TEXT NULL,
    cluster TEXT NULL,
    PRIMARY KEY (root_hash, cluster)
    FOREIGN KEY (root_hash) REFERENCES normalized_hashes (root_hash)
);
