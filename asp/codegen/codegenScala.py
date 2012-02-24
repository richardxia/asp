from ast import *
from ast_tools import *
import codegen
import scala_ast

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

TYPES = {
    'int' : 'Int',
    'float': 'Float',
    'double': 'Double',
    'string': 'String', 
    'boolean': 'Boolean',
    }
"""
POSSIBLE TYPES:
int
float
double
string
(array, type) i.e. (array, int)
boolean
specific class name
"""


ALL_SYMBOLS = {}
ALL_SYMBOLS.update(BOOLOP_SYMBOLS)
ALL_SYMBOLS.update(BINOP_SYMBOLS)
ALL_SYMBOLS.update(CMPOP_SYMBOLS)
ALL_SYMBOLS.update(UNARYOP_SYMBOLS)

def to_source(node):
    types = {}
    generator = SourceGenerator()
    generator.visit(node)
    
    return ''.join(generator.result)

global types
types = {}


def convert_types(input_type):
    if len(input_type) == 2 and input_type[0] == 'array':
        return 'org.apache.avro.generic.GenericData.Array[%s]' % (convert_types(input_type[1]))
    elif input_type in TYPES:
        return TYPES[input_type]
    else:
        print 'WARNING POTENTIAL SCALA TYPE MISMATCH'
        return input_type

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
  
    types = {}
    
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
        if isinstance(node, Call) and self.new_lines ==-1:
            self.new_lines = 0
        else:
            self.new_lines = max(self.new_lines, 1 + extra)


    def body(self, statements):
        self.new_line = True
        self.indentation += 1
        for stmt in statements:
            self.visit(stmt)
        self.indentation -= 1

    def visit_func_types(self,node):
        source = []
        for t in node.types:
            source.append(eval(codegen.to_source(t)))
        for func in source:
            name = func[0]
            #convert types somewhere?
            scala_arg_types, scala_ret_type = [],[]
            for arg in func[1]:
                scala_arg_types.append(convert_types(arg))
            scala_ret_type = convert_types(func[2])
            types[name] = [scala_arg_types, scala_ret_type]    
        
    def visit_Number(self, node):
        self.write(repr(node.num))

    def visit_String(self, node):
        self.write(repr(node.text))
    
    def visit_Name(self, node):
        self.write(node.name)

    def visit_Expression(self, node):
        self.newline(node) #may cause problems in newline()
        self.generic_visit(node)

    def visit_BinOp(self, node):
        self.visit(node.left)
        self.write(' ' + node.op + ' ')
        self.visit(node.right)
    
    def visit_BoolOp(self,node):
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
    
    def visit_UnaryOp(self, node):
        self.write('(')
        op = UNARYOP_SYMBOLS[type(node.op)]  #needs to be fixed. won't work
        self.write(op)  
        if op == 'not':
            self.write(op)
        if op == 'not':
            self.write(' ')
        self.visit(node.operand)
        self.write(')')

    def visit_Subscript(self, node):
        self.visit(node.value)
        self.write('[')
        self.visit(node.index) #???
        self.write(']')
        
        
    #what about newline stuff?? sort of n    
    #will need to replace outer 's with "" s ...
    #to do the above, for SString add a flag that if set the 's are removed
    
    def visit_Print(self, node):
        self.newline(node)
        self.write('println(')
        for t in node.text: 
            #print 'T IN TEXT IS:', t.text     
            self.visit(t)
            if t != node.text[-1]:
                self.write('+" " + ')
        self.write(')')

    def visit_List(self, node):
        elmts = node.elements
        self.write('Array(')
        for e in elmts:
            self.visit(e)
            if e != elmts[-1]:
                self.write(',')
        self.write(')')
    
    def visit_Attribute(self,node):
        value = self.visit(node.value)
        self.write('.' + node.attr)
        
    def visit_Sub(self,node):  
        self.visit(node.value)
        self.write('(')      
        self.visit(node.slice)
        self.write(')')
    
    def visit_Call(self,node):
        self.newline(node)
        self.visit(node.func)
        self.write('(')
        for a in node.args:
            self.visit(a)
            if a != node.args[-1]:
                self.write(', ')
        self.write(')')
        
    def visit_Function(self,node):
        self.newline(node)
        self.visit(node.declaration)
        self.write('{ ')
        self.body(node.body)
        self.write("\n}")
    
    def visit_FunctionDeclaration(self,node):
        self.write('def '+node.name+'( ')        
        
        arg_types = types[node.name][0]
        ret_type = types[node.name][1]
        
        self.visit_Arguments(node.args, arg_types)
        self.write('): %s =' %(ret_type))
        
    def visit_Arguments(self,node, types=None):        
        for i in range(len(node.args)):
            arg = node.args[i]
            self.visit(arg)
            if types:
                self.write(': %s' %types[i])
            else:
                self.write(': Any')
            if arg!= node.args[-1]:
                self.write(', ')

    
    def visit_ReturnStatement(self, node):
        self.newline(node)
        self.write('return ')
        self.visit(node.retval)
        
    def visit_Compare(self,node):
        self.newline(node,-1)
        self.write('(')
        self.visit(node.left)
        self.write(' %s ' %(node.op))
        self.visit(node.right)
        self.write(')')
    
    def visit_AugAssign(self,node):
        self.newline(node)
        self.visit(node.target)
        self.write(' ' + node.op +'= ')
        self.visit(node.value)
        
    def visit_Assign(self,node):
        try:
            if node.lvalue.name == 'TYPE_DECS':
                self.visit(node.rvalue)
                return 0
        except:
            pass
        
        self.newline(node)       
        if not isinstance(node.lvalue, Sub):
            self.write('var ')
        #print 'NODE LVALUE:', type(node.lvalue)
        self.visit(node.lvalue)
        self.write(' = ')
        
        self.new_lines = -1
        self.visit(node.rvalue)
        self.new_lines = 0
    
    def visit_IfConv(self,node):
        self.newline(node)
        if node.inner_if:
            if isinstance(node.orelse[0], IfConv) :
                self.write('else if (')                           
                
            else:
                self.write('else if (')
                self.visit(node.test)
                self.write(') {')
                self.body(node.body)
                self.newline(node)
                self.write("}")
                self.newline(node)
                self.write('else {')
                self.body(node.orelse)
                self.newline(node)
                self.write('}')
                return
        else:       
            self.write('if (')
            
        self.visit(node.test)
        self.write(') {')
        self.body(node.body)
        self.newline(node)
        self.write('}')
        
        if node.orelse:
            self.newline(node)
            self.body(node.orelse)
    
    def visit_For(self,node):
        self.newline(node)
        self.write('for (')
        self.visit(node.target)
        self.write( ' <- ')
        self.visit(node.iter)
        self.write(') {')
        self.body(node.body)
        self.newline(node)
        self.write('}')
    
    def visit_While(self, node):
        self.newline(node)
        self.write('while (')
        self.new_lines = -234234
        self.visit(node.test)
        self.write(') {')
        self.newline(node)
        self.body(node.body)
        self.newline(node)
        self.write('}')
    
    
    
    
    