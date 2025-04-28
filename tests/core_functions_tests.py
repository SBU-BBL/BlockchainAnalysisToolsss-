import unittest

#########################################################################################################################
import unittest
# These unit tests are designed to test that the parser works for all patters and nested lists as expected.
class testParseDesc(unittest.TestCase):
    def test_pk_single_key(self):
        desc = "pk(0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798)"
        expected = ["0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"]
        self.assertEqual(parseDesc(desc), expected)

    def test_multi_multiple_keys(self):
        desc = "multi(2,022f8bde4d1a07209355b4a7250a5c5128e88b84bddc619ab7cba8d569b240efe4,025cbdf0646e5db4eaa398f365f2ea7a0e3d419b7e0330e39ce92bddedcac4f9bc)"
        expected = [
            "022f8bde4d1a07209355b4a7250a5c5128e88b84bddc619ab7cba8d569b240efe4",
            "025cbdf0646e5db4eaa398f365f2ea7a0e3d419b7e0330e39ce92bddedcac4f9bc"
        ]
        self.assertEqual(parseDesc(desc), expected)

    def test_pkh_with_origin_and_derivation(self):
        desc = "pkh([d34db33f/44'/0'/0']xpub6ERApfZs7RXYLQSM3Xg3J2UhQn9A.../1/*)"
        # Only the key part should remain after parsing (without the origin info)
        keys = parseDesc(desc)
        self.assertEqual(len(keys), 1)
        self.assertTrue(keys[0].startswith("xpub6ERApfZs7RXYLQSM3Xg3J2UhQn9A"))
        self.assertIn("/1/*", keys[0])

    def test_tr_with_nested_scripts(self):
        # Taproot descriptor with a nested pk
        desc = "tr(c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5,{pk(fff97bd5755eeea420453a14355235d382f6472f8568a18b2f057a1460297556)})"
        expected = [
            "c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5",
            "fff97bd5755eeea420453a14355235d382f6472f8568a18b2f057a1460297556"
        ]
        self.assertEqual(parseDesc(desc), expected)

    def test_wpkh_with_extended_key(self):
        desc = "wpkh(xpub661MyMwAqRbcF8YnRxELb8eQpjY..."
        desc += "/0/*)"
        keys = parseDesc(desc)
        self.assertEqual(len(keys), 1)
        self.assertIn("xpub661MyMwAqRbcF8YnRxELb8eQpjY", keys[0])
        self.assertIn("/0/*", keys[0])

    def test_checksum_removal(self):
        desc = "sh(wpkh(02f9308a019258c31049344f85f89d5229b531c845836f99b08601f113bce036f9))#abcd1234"
        expected = ["02f9308a019258c31049344f85f89d5229b531c845836f99b08601f113bce036f9"]
        self.assertEqual(parseDesc(desc), expected)


class TestDeriveUndefinedAddresses(unittest.TestCase):
    
    def test_compressed_pubkey(self):
        compressed_key = "02e3af28965693b9ce1228f9d468149b831d6a0540b25e8a9900f71372c11fb277"
        expected = [
            '04e3af28965693b9ce1228f9d468149b831d6a0540b25e8a9900f71372c11fb277f9638aec40320be2eab90a7c245f2a40e248f7748f9991ea121eea23a0498d12',
            '02e3af28965693b9ce1228f9d468149b831d6a0540b25e8a9900f71372c11fb277',
            '1MHk5o3iammySYyowWcmRPTtAiQAfkhJnS',
            '13mKVN2PVGYdNLSLG8egVXwnPFrSUtWCTE',
            'bc1qreglehq5h6dpfzas4tkfr9ltglyrwahmg0g5r6'
        ]
        result = deriveUndefinedAddresses(compressed_key)
        self.assertEqual(result, expected)

    def test_uncompressed_pubkey(self):
        uncompressed_key = "042f90074d7a5bf30c72cf3a8dfd1381bdbd30407010e878f3a11269d5f74a58788505cdca22ea6eab7cfb40dc0e07aba200424ab0d79122a653ad0c7ec9896bdf"
        expected = [
            '042f90074d7a5bf30c72cf3a8dfd1381bdbd30407010e878f3a11269d5f74a58788505cdca22ea6eab7cfb40dc0e07aba200424ab0d79122a653ad0c7ec9896bdf',
            '032f90074d7a5bf30c72cf3a8dfd1381bdbd30407010e878f3a11269d5f74a5878',
            '1Fz5s6qVFwP3MDGeNav4ESQXFMpm8ELzUw',
            '1G2szusZxDPNvLfdLmoY8XqZDKFKDpZ6Hj',
            'bc1q5njdl3f9axvaaekma22ven2dxld9fjdeaxa898'
        ]
        result = deriveUndefinedAddresses(uncompressed_key)
        self.assertEqual(result, expected)

    def test_multisig_pubkey_assume_owned_true(self):
        multisig_keys = [
            "022afc20bf379bc96a2f4e9e63ffceb8652b2b6a097f63fbee6ecec2a49a48010e",
            "03a767c7221e9f15f870f1ad9311f5ab937d79fcaeee15bb2c722bca515581b4c0"
        ]
        expected = [
            '042afc20bf379bc96a2f4e9e63ffceb8652b2b6a097f63fbee6ecec2a49a48010e2cb891c04968f2b98e4d803100c01206b2974eae23873aac5bb6769964fec510',
            '022afc20bf379bc96a2f4e9e63ffceb8652b2b6a097f63fbee6ecec2a49a48010e',
            '1FpModLreXkvXJNM8gbjDn8YMcvXsynVct',
            '1Bt8XZ3RDUUsRmmqM26uCfNxQF6SEyrjvt',
            'bc1qwawvzs3lp99xk0wg4qqkq0n27ey3pdm53pmnc4',
            '04a767c7221e9f15f870f1ad9311f5ab937d79fcaeee15bb2c722bca515581b4c02fb838c9066bd0448ba3c0e734ab987e970b42f40c5b8571a05adcd7d94c72e5',
            '03a767c7221e9f15f870f1ad9311f5ab937d79fcaeee15bb2c722bca515581b4c0',
            '1JziWdpY7H8y5hDmCCCoY9Go1WE49SrLyB',
            '12ppVrt7pVMQnVpekHmrcEZ5vUnUcFfV6w',
            'bc1qzsp4xvdgf2xeh277vcu306824t9n3e2k8e43jm'
        ]
        result = deriveUndefinedAddresses(multisig_keys, assume_multisig_owned=True)
        self.assertEqual(result, expected)

    def test_multisig_pubkey_assume_owned_false(self):
        multisig_keys = [
            "022afc20bf379bc96a2f4e9e63ffceb8652b2b6a097f63fbee6ecec2a49a48010e",
            "03a767c7221e9f15f870f1ad9311f5ab937d79fcaeee15bb2c722bca515581b4c0"
        ]
        expected = [
            [
                '042afc20bf379bc96a2f4e9e63ffceb8652b2b6a097f63fbee6ecec2a49a48010e2cb891c04968f2b98e4d803100c01206b2974eae23873aac5bb6769964fec510',
                '022afc20bf379bc96a2f4e9e63ffceb8652b2b6a097f63fbee6ecec2a49a48010e',
                '1FpModLreXkvXJNM8gbjDn8YMcvXsynVct',
                '1Bt8XZ3RDUUsRmmqM26uCfNxQF6SEyrjvt',
                'bc1qwawvzs3lp99xk0wg4qqkq0n27ey3pdm53pmnc4'
            ],
            [
                '04a767c7221e9f15f870f1ad9311f5ab937d79fcaeee15bb2c722bca515581b4c02fb838c9066bd0448ba3c0e734ab987e970b42f40c5b8571a05adcd7d94c72e5',
                '03a767c7221e9f15f870f1ad9311f5ab937d79fcaeee15bb2c722bca515581b4c0',
                '1JziWdpY7H8y5hDmCCCoY9Go1WE49SrLyB',
                '12ppVrt7pVMQnVpekHmrcEZ5vUnUcFfV6w',
                'bc1qzsp4xvdgf2xeh277vcu306824t9n3e2k8e43jm'
            ]
        ]
        result = deriveUndefinedAddresses(multisig_keys, assume_multisig_owned=False)
        self.assertEqual(result, expected)
