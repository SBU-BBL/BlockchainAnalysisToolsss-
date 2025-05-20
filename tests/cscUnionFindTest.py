import unittest

class cscUnionFindTest(unittest.TestCase):

    def normalizeGroups(self, groups):
        "This function just sorts the groups so that its uniform - not necessary of the function."
        return sorted([sorted(group) for group in groups])

    def testSingleGroup(self):
        data = np.array([
            [1, 2],
            [1, 3],
            [1, 4]
        ], dtype=np.int32)
        expected = [[2, 3, 4]]
        result = cscUnionFind(data)
        self.assertEqual(self.normalizeGroups(result), self.normalizeGroups(expected))

    def testMultipleGroups(self):
        data = np.array([
            [1, 2],
            [1, 3],
            [2, 5],
            [2, 6],
            [10, 9],
            [10, 8]
        ], dtype=np.int32)
        expected = [[2, 3], [5, 6], [8, 9]]
        result = cscUnionFind(data)
        self.assertEqual(self.normalizeGroups(result), self.normalizeGroups(expected))

    def testDisconnectedGroups(self):
        data = np.array([
            [1, 2],
            [2, 3],
            [3, 4],
            [4, 5]
        ], dtype=np.int32)
        expected = [[2], [3], [4], [5]]
        result = cscUnionFind(data)
        self.assertEqual(self.normalizeGroups(result), self.normalizeGroups(expected))

    def testChainConnection(self):
        data = np.array([
            [1, 2],
            [1, 3],
            [2, 3],
            [2, 4],
            [3, 5],
            [5, 3],
            [5, 10]
        ], dtype=np.int32)
        expected = [[2, 3, 4, 10], [5]]
        result = cscUnionFind(data)
        self.assertEqual(self.normalizeGroups(result), self.normalizeGroups(expected))

    def testEmptyInput(self):
        data = np.empty((0, 2), dtype=np.int32)
        expected = []
        result = cscUnionFind(data)
        self.assertEqual(result, expected)

    def testRepeatedWallets(self):
        data = np.array([
            [1, 2],
            [1, 3],
            [1, 2],  
            [1, 3],  
            [1, 4]
        ], dtype=np.int32)
        expected = [[2, 3, 4]]
        result = cscUnionFind(data)
        self.assertEqual(self.normalizeGroups(result), self.normalizeGroups(expected))

if __name__ == '__main__':
    unittest.main()
    
