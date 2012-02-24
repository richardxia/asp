#!/usr/bin/env python

from array_doubler import *
import time

"""
import asp.codegen.ast_tools as ast_tools

#import ast_tools
from scala_ast import *
from cpp_ast import *
import codegen
import codegenScala
import ast
""" 

print 'start test'
arr = [1,2,3,4]
result = ArrayDoubler().double_py2scala(arr)
print 'done with test and result is:', result


"""

print "warmup"
start = time.time()
arr = [i*1.0 for i in range(100)]
result = ArrayDoubler().double_using_scala(arr)

end = time.time()
elapsed1 = end-start
print "done with warmup:", elapsed1



print "1000 elem"
start = time.time()
arr = [i*1.0 for i in range(1000)]
result = ArrayDoubler().double_using_scala(arr)

end = time.time()
elapsed1 = end-start
print "done with 1000 elem:", elapsed1


print "10000 elem"
start = time.time()
arr = [i*1.0 for i in range(10000)]
result = ArrayDoubler().double_using_scala(arr)

end = time.time()
elapsed1 = end-start
print "10,0000 finished:", elapsed1


print "100,000 elements!"
start = time.time()
arr = [i*1.0 for i in range(100000)]
result = ArrayDoubler().double_using_scala(arr)

end = time.time()
elapsed1 = end-start
print "done with 100,000 elem:", elapsed1
"""
"""
print "1,000,000 elem"
start = time.time()
arr = [i*1.0 for i in range(1000000)]
result = ArrayDoubler().double_using_scala(arr)

end = time.time()
elapsed1 = end-start
print "done with 1,000,000 elem:", elapsed1

"""


"""
print "beginning 10000 elements"
start = time.time()
arr = [i*1.0 for i in range(15000)]
result = ArrayDoubler().double_using_scala(arr)

end = time.time()
elapsed1 = end-start
print "elapsed time for 10000 is:", elapsed1
"""


"""

print "beginning 10000 elements"
start = time.time()
arr = [1.3 for i in range(10000)]
result = ArrayDoubler().double_using_scala(arr)

end = time.time()
elapsed2 = end-start
print "elapsed time for 10000 is:", elapsed2
print "10,000 is x times faster than 5 elements:" , elapsed/elapsed2
print "10,000 is x times faster than 1000 elements:" , elapsed1/elapsed2


"""