import unittest
import numpy as np
import numpy.testing as npt  

class testcscUnionFind(unittest.TestCase):

    def testSingleGroup(self):
        data = np.array([
            [1, 2],
            [1, 3],
            [1, 4]
        ], dtype=np.int32)
        expected = np.array([
            [2, 0],
            [3, 0],
            [4, 0]
        ], dtype=np.int32)
        result = cscUnionFind(data)
        npt.assert_array_equal(result, expected)

    def testMultipleGroups(self):
        data = np.array([
            [1, 2],
            [1, 3],
            [2, 5],
            [2, 6],
            [10, 9],
            [10, 8]
        ], dtype=np.int32)
        expected = np.array([
            [2, 0],
            [3, 0],
            [5, 1],
            [6, 1],
            [8, 2],
            [9, 2]
        ], dtype=np.int32)
        result = cscUnionFind(data)
        npt.assert_array_equal(result, expected)

    def testDisconnectedGroups(self):
        data = np.array([
            [1, 2],
            [2, 3],
            [3, 4],
            [4, 5]
        ], dtype=np.int32)
        expected = np.array([
            [2, 0],
            [3, 1],
            [4, 2],
            [5, 3]
        ], dtype=np.int32)
        result = cscUnionFind(data)
        npt.assert_array_equal(result, expected)

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
        expected = np.array([
            [2, 0],
            [3, 0],
            [4, 0],
            [5, 1],
            [10, 0]
        ], dtype=np.int32)
        result = cscUnionFind(data)
        npt.assert_array_equal(result, expected)

    def testRepeatedWallets(self):
        data = np.array([
            [1, 2],
            [1, 3],
            [1, 2],  
            [1, 3],  
            [1, 4]
        ], dtype=np.int32)
        expected = np.array([
            [2, 0],
            [3, 0],
            [4, 0]
        ], dtype=np.int32)
        result = cscUnionFind(data)
        npt.assert_array_equal(result, expected)

if __name__ == '__main__':
    unittest.main()
