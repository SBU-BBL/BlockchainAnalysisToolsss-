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
