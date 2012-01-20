from ast import *
from ast_tools import *

BOOLOP_SYMBOLS = {
    And:        'and',
    Or:         'or'
}

BINOP_SYMBOLS = {
    Add:        '+',
    Sub:        '-',
    Mult:       '*',
    Div:        '/',
    FloorDiv:   '//',
    Mod:        '%',
    LShift:     '<<',
    RShift:     '>>',
    BitOr:      '|',
    BitAnd:     '&',
    BitXor:     '^'
}

CMPOP_SYMBOLS = {
    Eq:         '==',
    Gt:         '>',
    GtE:        '>=',
    In:         'in',
    Is:         'is',
    IsNot:      'is not',
    Lt:         '<',
    LtE:        '<=',
    NotEq:      '!=',
    NotIn:      'not in'
}

UNARYOP_SYMBOLS = {
    Invert:     '~',
    Not:        'not',
    UAdd:       '+',
    USub:       '-'
}

ALL_SYMBOLS = {}
ALL_SYMBOLS.update(BOOLOP_SYMBOLS)
ALL_SYMBOLS.update(BINOP_SYMBOLS)
ALL_SYMBOLS.update(CMPOP_SYMBOLS)
ALL_SYMBOLS.update(UNARYOP_SYMBOLS)

def to_source(node):
    generator = SourceGenerator()
    generator.visit(node)
    return ''.join(generator.result)

class SourceGenerator(NodeVisitor):

    """

    def write(self, x):
        if self.new_lines:
            if self.result:
                self.result.append('\n' * self.new_lines)
            self.result.append(self.indent_with * self.indentation)
            self.new_lines = 0
        self.result.append(x)

    """
  
  
    def __init__(self):
        self.result = []
        self.new_lines=0
        self.indentation =0
        self.indent_with=' ' * 4
        
    
    def write(self,x):
        if self.new_lines:
            if self.result:
                self.result.append('\n' * self.new_lines)
            self.result.append(self.indent_with * self.indentation)
            self.new_lines = 0
        self.result.append(x)
        
    def newline(self, node=None, extra=0):
        self.new_lines = max(self.new_lines, 1 + extra)


    def body(self, statements):
        self.new_line = True
        self.indentation += 1
        for stmt in statements:
            self.visit(stmt)
        self.indentation -= 1

    def visit_SNumber(self, node):
        self.write(repr(node.num))

    def visit_SString(self, node):
        self.write(repr(node.text))
    
    def visit_SName(self, node):
        self.write(node.name)

    def visit_SExpression(self, node):
        self.newline(node) #may cause problems in newline()
        self.generic_visit(node)

    def visit_SBinOp(self, node):
        self.visit(node.left)
        self.write(' ' + node.op + ' ')
        self.visit(node.right)
    
    def visit_SBoolOp(self,node):
        self.newline(node)
        self.write('(')
        op = BOOLOP_SYMBOLS[type(node.op)]             
        self.visit(node.values[0])
        if op == 'and':
            self.write(' && ')
        elif op == 'or':
            self.write(' || ')
        else:
            raise Error("Unsupported BoolOp type")
        
        self.visit(node.values[1])
        self.write(')')   
    
    def visit_SUnaryOp(self, node):
        self.write('(')
        op = UNARYOP_SYMBOLS[type(node.op)]  #needs to be fixed. won't work
        self.write(op)  
        if op == 'not':
            self.write(op)
        if op == 'not':
            self.write(' ')
        self.visit(node.oeprand)
        self.write(')')

    def visit_SSubscript(self, node):
        self.visit(node.value)
        self.write('[')
        self.visit(node.index) #???
        self.write(']')
        
        
    #what about newline stuff?? sort of n    
    #will need to replace outer 's with "" s ...
    #to do the above, for SString add a flag that if set the 's are removed
    def visit_SPrint(self, node):
        self.newline(node)
        self.write('println(')
        for t in node.text: 
            #print 'T IN TEXT IS:', t.text     
            self.visit(t)
            if t != node.text[-1]:
                self.write('+" " + ')
        self.write(')')

    def visit_SList(self, node):
        elmts = node.elements
        self.write('Array(')
        for e in elmts:
            self.visit(e)
            if e != elmts[-1]:
                self.write(',')
        self.write(')')
        
    def visit_SReturnStatement(self, node):
        self.newline(node)
        self.write('return ')
        self.visit(node.retval)
        
    def visit_SCompare(self,node):
        self.newline(node,-1)
        self.write('(')
        self.visit(node.left)
        self.write(' %s ' %(node.op))
        self.visit(node.right)
        self.write(')')
    
    def visit_SAssign(self,node):
        self.newline(node)
        self.write('var ')
        self.visit(node.lvalue)
        self.write(' = ')
        self.visit(node.rvalue)
    
    def visit_SIfConv(self,node):
        self.newline(node)
        self.write('if (')
        self.visit(node.test)
        self.write(') {')
        self.body(node.body)
        self.newline(node)
        self.write('}')
        if node.orelse:
            self.body(node.orelse)
    
    def visit_SFor(self,node):
        self.newline(node)
        self.write('for (')
        self.visit(node.target)
        self.write( ' <- ')
        self.visit(node.iter)
        self.write(') {')
        self.body(node.body)
        self.newline(node)
        self.write('}')
    
    def visit_SWhile(self, node):
        self.newline(node)
        self.write('while (')
        self.new_lines = -234234
        print 'NODE TEST:', node.test
        self.visit(node.test)
        self.write(') {')
        self.newline(node)
        self.body(node.body)
        self.newline(node)
        self.write('}')
    
    
    
    
    