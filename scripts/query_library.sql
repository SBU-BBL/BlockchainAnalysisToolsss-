-- This query matches finds the matching inputs corresponding to an output txn.
-- This should not be explicitly stored in the database, as it breaks normal form (derives something trivial).
UPDATE inputs
SET vin_wallet_ID = COALESCE(
    (SELECT nh.root_hash
     FROM output_hashes oh
     JOIN normalized_hashes nh
     ON oh.address = nh.hash
     WHERE oh.txid = inputs.vin_txid AND oh.vout_n = inputs.vin_vout),
    (SELECT oh.address
     FROM output_hashes oh
     WHERE oh.txid = inputs.vin_txid AND oh.vout_n = inputs.vin_vout)
);
-- This query returns a blocktime indexed table of each clusters bitcoin balances.
