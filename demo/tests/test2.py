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
    def test2(self):
        class TestKernel(StencilKernel):
            def kernel(self, in_grid, out_grid):
                for x in out_grid.interior_points():
                    for y in in_grid.neighbors(x, 0):
                        out_grid[x] = 2*out_grid[x] + in_grid[y]

        kernel = TestKernel()
        in_grid = StencilGrid([10,10])
        in_grid.neighbor_definition = [[ (-1,0), (1,0), (0,-1), (0,1) ]]
        out_grid = StencilGrid([10,10])
        in_grid[2,3] = 1
        in_grid[4,3] = 2
        in_grid[3,2] = 3
        in_grid[3,4] = 4
        kernel.pure_python = True

        # INSTRUMENT KERNEL CODE FOR REPLAYING TRACES
        #kernel.should_trace = True

        # INSTRUMENT KERNEL CODE FOR REPLAYING TRACES
        kernel.kernel(in_grid, out_grid)

        # REPLAY PREVIOUS TRACE IN PYTHON LEVEL
        #replay_out_grid = StencilGrid([10,10])
        #kernel.replay(in_grid, replay_out_grid)

        # CHECK SOME CONDITION ON OUTPUT
        self.assertEqual(out_grid[3,3], 26)

if __name__ == '__main__':
    unittest.main()
