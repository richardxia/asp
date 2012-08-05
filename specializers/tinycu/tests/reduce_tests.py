import unittest2 as unittest
from tinycu import *

# Redefine Python's reduce to return a list
old_reduce = reduce
def reduce(*args):
    return [old_reduce(*args)]

class ReduceTests(unittest.TestCase):
    
    def test_sum(self):
        class MyReduceTest(TinyCu):
            def func(self, a, b):
                return a + b
            def __call__(self, *args):
                return reduce(self.func, [1,2,3,4])

        b = MyReduceTest()
        result = b.intercept_call()
        print(result)
        self.assertEqual(result, [10])

    def test_sum16(self):
        class MyReduceTest(TinyCu):
            def func(self, a, b):
                return a + b
            def __call__(self, *args):
                return reduce(self.func, [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16])

        b = MyReduceTest()
        result = b.intercept_call()
        print(result)
        self.assertEqual(result, [136])

    def test_nonassociative(self):
        class MyReduceTest(TinyCu):
            def func(self, a, b):
                return a + 2 * b
            def __call__(self, *args):
                return reduce(self.func, [1,2,3,4])

        b = MyReduceTest()
        result = b.intercept_call()
        print(result)
        self.assertEqual(result, [19])

if __name__ == '__main__':
    unittest.main()

