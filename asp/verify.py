import ast
import asp.codegen.cpp_ast as cpp_ast

class NodeFinder(ast.NodeVisitor):
    '''Generic class for finding a specific node in an AST subtree.'''
    def __init__(self, predicate):
        self.predicate = predicate

    def generic_visit(self, node):
        if self.predicate(node):
            return node
        for field, value in ast.iter_fields(node):
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, ast.AST):
                        tmp = self.visit(item)
                        if tmp:
                            return tmp
            elif isinstance(value, ast.AST):
                tmp = self.visit(value)
                if tmp:
                    return tmp
        return False

class AspTrace(object):
    def __init__(self, trace_file="/tmp/trace.txt"):
        self.trace_file = trace_file
        self.var_name = '_asp_log_file'

    def emit_open(self):
        block = cpp_ast.UnbracedBlock()
        block.append(cpp_ast.Value("std::ofstream", self.var_name))
        block.append(cpp_ast.FunctionCall("%s.open" % self.var_name, [cpp_ast.String(self.trace_file)]))
        return block

    def emit_close(self):
        return cpp_ast.FunctionCall("%s.close" % self.var_name, [])

    def emit_write(self, *args):
        return cpp_ast.PrintLog.write_log(*args)
