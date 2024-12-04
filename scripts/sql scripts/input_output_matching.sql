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
