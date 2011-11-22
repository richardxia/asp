import unittest2 as unittest
import ast
import math
import itertools
from stencil_kernel import *
from stencil_python_front_end import *
from stencil_unroll_neighbor_iter import *
from stencil_convert import *
from asp.util import *
class BasicTests(unittest.TestCase):
    def test_init(self):
        # if no kernel method is defined, it should fail
        self.failUnlessRaises((Exception), StencilKernel)
    
    #def test_pure_python(self):
    #    class IdentityKernel(StencilKernel):
    #        def kernel(self, in_grid, out_grid):
    #            for x in out_grid.interior_points():
    #                out_grid[x] = in_grid[x]

    #    kernel = IdentityKernel()
    #    in_grid = StencilGrid([10,10])
    #    out_grid = StencilGrid([10,10])
    #    kernel.pure_python = True
    #    kernel.kernel(in_grid, out_grid)
    #    self.failIf(in_grid[3,3] != out_grid[3,3])

    # Don't modify the ast, just overload the iterators?
    
    def test_parallelization(self):
        class IdentityKernel(StencilKernel):
            def kernel(self, in_grid, out_grid):
                for x in out_grid.interior_points():
                    for y in in_grid.neighbors(x, 1):
                        out_grid[x] = out_grid[x] + in_grid[y]

        kernel = IdentityKernel()
        in_grid = StencilGrid([10,10])
        out_grid = StencilGrid([10,10])
        in_grid[2,3] = 4
        in_grid[3,2] = 1
        in_grid[3,4] = 2
        in_grid[4,3] = 3
        kernel.logging = True
        #kernel.kernel(in_grid, out_grid)

        out_grid = StencilGrid([10,10])
        kernel = IdentityKernel()
        kernel.verify_log = True
        kernel.kernel(in_grid, out_grid)
        #self.failIf(in_grid[3,3] != out_grid[3,3])

if __name__ == '__main__':
    unittest.main()
