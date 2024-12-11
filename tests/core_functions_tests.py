import unittest
class testUnionFind(unittest.TestCase):
    def test_unionFind(self):
        # Input lists for testing
        input_lists = [
            ['ABC', 'DEF', 'GHI'],
            ['ZZZ', 'JAI', 'ABC'],
            ['JKA', 'KAP', 'LALPA'],
            ['DEF', 'KLO']
        ]
        # Sort in case algorithm changes - this ensures generalization.
        expected_output = [
            sorted(['ABC', 'DEF', 'GHI', 'ZZZ', 'JAI', 'KLO']),
            sorted(['JKA', 'KAP', 'LALPA'])
        ]
        examined = unionFind(input_lists)
        examined_sorted = [sorted(group) for group in examined]

        # Sort the outer list for consistent comparison
        examined_sorted.sort()
        expected_output.sort()

        # Assert the result matches the expected output
        self.assertEqual(examined_sorted, expected_output)

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
