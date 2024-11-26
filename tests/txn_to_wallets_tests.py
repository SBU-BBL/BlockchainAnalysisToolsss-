# -*- coding: utf-8 -*-


class testparsePushData(unittest.TestCase):

    def testpPK(self):
      specScripts = {
    'pubkeytest': {
        "script_asm": "0496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858ee OP_CHECKSIG",
        "expected": ["0496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858ee"]
    },
    "multisigtest": {
        "script_asm":  "OP_2 OP_PUSHBYTES_65 04d81fd577272bbe73308c93009eec5dc9fc319fc1ee2e7066e17220a5d47a18314578be2faea34b9f1f8ca078f8621acd4bc22897b03daa422b9bf56646b342a2 OP_PUSHBYTES_65 04ec3afff0b2b66e8152e9018fe3be3fc92b30bf886b3487a525997d00fd9da2d012dce5d5275854adc3106572a5d1e12d4211b228429f5a7b2f7ba92eb0475bb1 OP_PUSHBYTES_65 04b49b496684b02855bc32f5daefa2e2e406db4418f3b86bca5195600951c7d918cdbe5e6d3736ec2abf2dd7610995c3086976b2c0c7b4e459d10b34a316d5a5e7 OP_3 OP_CHECKMULTISIG",
        "expected": ["04d81fd577272bbe73308c93009eec5dc9fc319fc1ee2e7066e17220a5d47a18314578be2faea34b9f1f8ca078f8621acd4bc22897b03daa422b9bf56646b342a2",
                     "04ec3afff0b2b66e8152e9018fe3be3fc92b30bf886b3487a525997d00fd9da2d012dce5d5275854adc3106572a5d1e12d4211b228429f5a7b2f7ba92eb0475bb1",
                     "04b49b496684b02855bc32f5daefa2e2e406db4418f3b86bca5195600951c7d918cdbe5e6d3736ec2abf2dd7610995c3086976b2c0c7b4e459d10b34a316d5a5e7"]
        }
    }
        for test_name, test_case in specScripts.items():
            with self.subTest(test=test_name):
                result = parsePushData(test_case['script_asm'])
                self.assertEqual(result, test_case['expected'])

txns_csv_path =
matched_expected_path =
class testmatchAddresses(unittest.TestCase):

    def setUp(self):
        # Load the input data from txns.csv and the expected output from matched_addresses.csv
        self.input_df = pd.read_csv(txns_csv_path)
        self.expected_output_df = pd.read_csv(matched_expected_path) # I'll make these default later

    def test_match_addresses(self):
        # Run the function on the input data
        output_df = matchAddresses(self.input_df.copy())

        # Compare the output dataframe to the expected dataframe
        pd.testing.assert_frame_equal(output_df, self.expected_output_df, check_dtype=False)

if __name__ == '__main__':
    unittest.main()
