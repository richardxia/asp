"""The main driver, intercepts the kernel() call and invokes the other components.

Stencil kernel classes are subclassed from the StencilKernel class
defined here. At initialization time, the text of the kernel() method
is parsed into a Python AST, then converted into a StencilModel by
stencil_python_front_end. The kernel() function is replaced by
shadow_kernel(), which intercepts future calls to kernel().

During each call to kernel(), stencil_unroll_neighbor_iter is called
to unroll neighbor loops, stencil_convert is invoked to convert the
model to C++, and an external compiler tool is invoked to generate a
binary which then efficiently completes executing the call. The binary
is cached for future calls.
"""

import numpy
import inspect
from stencil_grid import *
from stencil_python_front_end import *
from stencil_unroll_neighbor_iter import *
from stencil_optimize_cpp import *
from stencil_convert import *
import asp.codegen.python_ast as ast
import asp.codegen.cpp_ast as cpp_ast
import asp.codegen.ast_tools as ast_tools
from asp.util import *
import copy

from stencil_logging_transform import StencilLoggingTransform
from stencil_logging_transform import _log_read
from asp.verify import SpecializationError

# (elmas): for the c++ parallelism analysis
from asp.analysis.cpp_instrument import *

def get_func(code, fname, binder=None):
    exec code
    func = eval(fname)
    if binder is not None:
        import types
        return types.MethodType(func, binder)
    return func

def instrument_grids(out_grid, in_grids):
    class LoggedStencilGrid(StencilGrid):
        def __setitem__(self, attr, value):
            super(self.__class__, self).__setitem__(attr, value)
            if log.current_value != value:
                raise SpecializationError, "Expected index %s to have value %s, got %s" % (attr, log.current_value, value)

    def verifying_iterator(self, *args):
        old_values = set([x for x in self.old_neighbors(*args)])
        for value in log.getIterator()(self, *args):
            if value not in old_values:
                raise SpecializationError("Unexpected iteration value: " + str(value))
            else:
                old_values.remove(value)
                yield value

    log = _log_read()
    for in_grid in in_grids:
        import types
        in_grid.old_neighbors = in_grid.neighbors
        in_grid.neighbors = types.MethodType(verifying_iterator, in_grid)

    # Kind of ghetto to just change its class to the special type
    out_grid.__class__ = LoggedStencilGrid

# may want to make this inherit from something else...
class StencilKernel(object):
    def __init__(self, with_cilk=False):
        # we want to raise an exception if there is no kernel()
        # method defined.
        try:
            dir(self).index("kernel")
        except ValueError:
            raise Exception("No kernel method defined.")

        # get text of kernel() method and parse into a StencilModel
        self.kernel_src = inspect.getsource(self.kernel)
        # print(self.kernel_src)
        self.kernel_ast = ast.parse(self.remove_indentation(self.kernel_src))
        # print(ast.dump(self.kernel_ast, include_attributes=True))
        self.model = StencilPythonFrontEnd().parse(self.kernel_ast)
        # print(ast.dump(self.model, include_attributes=True))

        self.pure_python = False
        self.pure_python_kernel = self.kernel
        self.should_unroll = True
        self.should_cacheblock = False
        self.block_size = 1
        self.should_trace = False
        self.verify_log = False
        
        # replace kernel with shadow version
        self.kernel = self.shadow_kernel

        self.specialized_sizes = None
        self.with_cilk = with_cilk

    def replay(self, *args):
        self.verify_log = True
        try:
            self.kernel(*args)
        except SpecializationError as e:
            print("\n*************** Specialization Error:", e)
        else:
            print("\n*************** No Specialization Error:")

    def remove_indentation(self, src):
        return src.lstrip()

    def add_libraries(self, mod):
        # these are necessary includes, includedirs, and init statements to use the numpy library
        mod.add_library("numpy",[numpy.get_include()+"/numpy"])
        mod.add_header("arrayobject.h")
        mod.add_to_init([cpp_ast.Statement("import_array();")])
        if self.with_cilk:
            mod.module.add_to_preamble([cpp_ast.Include("cilk/cilk.h", True)])
        if self.should_trace:
            mod.backends['c++'].module.add_to_preamble([cpp_ast.Include("fstream", True)])
        

    def shadow_kernel(self, *args):
        if self.verify_log:
            # Figure out what the actual function prototype is
            instrument_grids(args[1], [args[0]])
            self.pure_python = True
        if self.pure_python:
            return self.pure_python_kernel(*args)

        #FIXME: instead of doing this short-circuit, we should use the Asp infrastructure to
        # do it, by passing in a lambda that does this check
        # if already specialized to these sizes, just run
        if self.specialized_sizes and self.specialized_sizes == [y.shape for y in args]:
            debug_print("match!")
            self.mod.kernel(*[y.data for y in args])
            return

        # otherwise, do the first-run flow

        # ask asp infrastructure for machine and platform info, including if cilk+ is available
        #FIXME: impelement.  set self.with_cilk=true if cilk is available

        if self.should_trace:
            self.model = StencilPythonFrontEnd(should_trace=self.should_trace).parse(self.kernel_ast)
        
        input_grids = args[0:-1]
        output_grid = args[-1]
        model = copy.deepcopy(self.model)
        model = StencilUnrollNeighborIter(model, input_grids, output_grid).run()

        # depending on whether cilk is available, we choose which converter to use
        if not self.with_cilk:
            Converter = StencilConvertAST
        else:
            Converter = StencilConvertASTCilk

        # generate variant with no unrolling, then generate variants for various unrollings
        base_variant = Converter(model, input_grids, output_grid, should_trace=self.should_trace).run()
        variants = [base_variant]
        variant_names = ["kernel"]

        # we only cache block if the size is large enough for blocking
        # or if the user has told us to
        
        if (len(args[0].shape) > 1 and args[0].shape[0] > 128):
            self.should_cacheblock = True
            self.block_sizes = [16, 32, 48, 64, 128, 160, 192, 256]
        else:
            self.should_cacheblock = False
            self.block_sizes = []

        if self.should_cacheblock and self.should_unroll:
            import itertools
            for b in list(set(itertools.permutations(self.block_sizes, len(args[0].shape)-1))):
                for u in [1,2,4,8]:
                    # ensure the unrolling is valid for the given blocking

                    #if b[len(b)-1] >= u:
                    if args[0].shape[len(args[0].shape)-1] >= u:
                        c = list(b)
                        c.append(1)
                        #variants.append(Converter(model, input_grids, output_grid, unroll_factor=u, block_factor=c).run())
                        
                        variant = StencilOptimizeCpp(copy.deepcopy(base_variant), output_grid.shape, unroll_factor=u, block_factor=c).run()
                        variants.append(variant)
                        variant_names.append("kernel_block_%s_unroll_%s" % ('_'.join([str(y) for y in c]) ,u))

                        debug_print("ADDING BLOCKED")
                        
        if self.should_unroll:
            for x in [2,4,8,16]: #,32,64]:
                check_valid = max(map(
                    # FIXME: is this the right way to figure out valid unrollings?
                    lambda y: (y.shape[-1]-2*y.ghost_depth) % x,
                    args))

                if check_valid == 0:
                    debug_print("APPENDING VARIANT %s" % x)
                    variants.append(StencilOptimizeCpp(copy.deepcopy(base_variant), output_grid.shape, unroll_factor=x).run())
                    variant_names.append("kernel_unroll_%s" % x)

        debug_print(variant_names)
        from asp.jit import asp_module

        mod = self.mod = asp_module.ASPModule()
        self.add_libraries(mod)

        self.set_compiler_flags(mod)

        ###########################################################################
        # TRANSFORMATION TO ADD PARALLEL-FOR LOOPS
        variants = cpp_parallel_for_loop_transformation(self, variants, variant_names)

        # "BUGGY" C++ LEVEL TRANSFORMATION (HOIST INT VARIABLES OUT OF FOR LOOP)
        variants = cpp_hoist_index_variable_transformation(self, variants, variant_names)

        # C++ LEVEL INSTRUMENTATION FOR CHECKING PARALLELISM ERRORS
        variants = cpp_instrument_for_analysis(self, variants, variant_names, mod)
        
        ###########################################################################
        
        mod.add_function("kernel", variants, variant_names)

        # package arguments and do the call 
        myargs = [y.data for y in args]
        mod.kernel(*myargs)

        # save parameter sizes for next run
        self.specialized_sizes = [x.shape for x in args]

    def set_compiler_flags(self, mod):
        import asp.config
        
        if self.with_cilk or asp.config.CompilerDetector().detect("icc"):
            mod.backends["c++"].toolchain.cc = "icc"
            mod.backends["c++"].toolchain.cflags += ["-intel-extensions", "-fast", "-restrict"]
            mod.backends["c++"].toolchain.cflags += ["-openmp", "-fno-fnalias", "-fno-alias"]
            mod.backends["c++"].toolchain.cflags += ["-I/usr/include/x86_64-linux-gnu"]
            mod.backends["c++"].toolchain.cflags.remove('-fwrapv')
            mod.backends["c++"].toolchain.cflags.remove('-O2')
            mod.backends["c++"].toolchain.cflags.remove('-g')
            mod.backends["c++"].toolchain.cflags.remove('-g')
            mod.backends["c++"].toolchain.cflags.remove('-fno-strict-aliasing')
        else:
            mod.backends["c++"].toolchain.cflags += ["-fopenmp", "-O3", "-msse3", "-Wno-unknown-pragmas"]

        if mod.backends["c++"].toolchain.cflags.count('-Os') > 0:
            mod.backends["c++"].toolchain.cflags.remove('-Os')
        if mod.backends["c++"].toolchain.cflags.count('-O2') > 0:
            mod.backends["c++"].toolchain.cflags.remove('-O2')
        debug_print("toolchain" + str(mod.backends["c++"].toolchain.cflags))
