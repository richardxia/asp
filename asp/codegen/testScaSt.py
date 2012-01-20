import ast_tools
from scala_ast import *
from cpp_ast import *
import codegen
import codegenScala
import ast
    
if __name__ == '__main__': 
    f = open('times4.py')
    rendered = f.read()
    print 'ORIGINAL RENDERED:', rendered
    print' DONE'
    astt = ast.parse(rendered)
    print 'ORIGINAL AST:', ast.dump(astt)
    print 'DONE'
    print 'ORIGINAL AST RENDERED:', codegen.to_source(astt)
    print 'DONE----------------------------------------------------'
    node = ast_tools.ConvertAST_ScalaAST().visit(astt)
    print '______________________________________-changed ast:', ast.dump(node)
    print 'DONE'
    print 'CHANGED AST RENDERED BY CODEGEN:', codegen.to_source(node)
    print 'CHANGED AST RENDERED BY SCALACODEGEN', codegenScala.to_source(node)
    
