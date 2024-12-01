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

class testParseDesc(unittest.TestCase):
    
    def test_single_key_scripts(self):
        self.assertEqual(
            parseDesc("pk(0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798)"),
            ["0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"]
        )
        self.assertEqual(
            parseDesc("pkh(02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5)"),
            ["02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5"]
        )
        self.assertEqual(
            parseDesc("wpkh(02f9308a019258c31049344f85f89d5229b531c845836f99b08601f113bce036f9)"),
            ["02f9308a019258c31049344f85f89d5229b531c845836f99b08601f113bce036f9"]
        )
    def test_addresses(self):
        self.assertEqual(
            parseDesc("addr(1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa)"),
            ["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"]
        )
        self.assertEqual(
            parseDesc("addr(bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080)"),
            ["bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080"]
        )
    def test_multisig(self):
        self.assertEqual(
            parseDesc("multi(1,022f8bde4d1a07209355b4a7250a5c5128e88b84bddc619ab7cba8d569b240efe4,025cbdf0646e5db4eaa398f365f2ea7a0e3d419b7e0330e39ce92bddedcac4f9bc)"),
            ["022f8bde4d1a07209355b4a7250a5c5128e88b84bddc619ab7cba8d569b240efe4",
             "025cbdf0646e5db4eaa398f365f2ea7a0e3d419b7e0330e39ce92bddedcac4f9bc"]
        )
        self.assertEqual(
            parseDesc("sh(multi(2,022f01e5e15cca351daff3843fb70f3c2f0a1bdd05e5af888a67784ef3e10a2a01,03acd484e2f0c7f65309ad178a9f559abde09796974c57e714c35f110dfc27ccbe))"),
            ["022f01e5e15cca351daff3843fb70f3c2f0a1bdd05e5af888a67784ef3e10a2a01",
             "03acd484e2f0c7f65309ad178a9f559abde09796974c57e714c35f110dfc27ccbe"]
        )
