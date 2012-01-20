import ast
import codegen


"""
I don't really seem to use the generate method
"""

class SGenerable():
	def generate(self):
		raise Exception("generate method not implemented")

	
class SNumber(SGenerable):
	def __init__(self, num):
		self.num = num
		self._fields = []

	def generate(self):
		yield str(self.num)

class SString(SGenerable):
	def __init__(self, text):
		self.text = text
		self._fields = ['text']
		self.done = False
	
	def __iter__(self):
		return self
	
	def next(self):
		if self.done:
			raise StopIteration
		else:
			self.done = True
			return self
	
	def generate(self):
		yield '\"%s\"' %self.text

class SName(SGenerable):
	def __init__(self,name):
		self.name= name
		self._fields= []
		self.done= False

	def __iter__(self):
		return self
	
	def next(self):
		if self.done:
			raise StopIteration
		else:
			self.done = True
			return self

	def generate(self):
		yield self.name

class SFunctionBody(SGenerable):
	pass

class SFunctionDec(SGenerable):
	pass

class Value(): #????
	pass

class SExpression(SGenerable):
	def __init__(self):
		# ???
		super(Expression, self)
		self._fields = []
		self.done= False

	def __iter__(self):
		return self
	
	def next(self):
		if self.done:
			raise StopIteration
		else:
			self.done = True
			return self
	
	def generate(self):
		yield ""

class SList(SExpression):
	def __init__(self, elements):
		self.elements = elements
		
		
class SBinOp(SExpression):
	def __init__(self, left, op, right):
		self. left = left
		self.op = op
		self.right = right
		self._fields = ['left', 'right']

	def generate(self):
		yield "(%s %s %s)" % (self.left, self.op, self.right)

class SBoolOp(SExpression):
	def __init__(self, op, values):
		self.op = op
		self.values = values
		self._fields = ['op', 'values']
		self.done= False

	def __iter__(self):
		return self
	
	def next(self):
		if self.done:
			raise StopIteration
		else:
			self.done = True
			return self

	def generate(self):
		yield "(%s %s %s)" % (self.left, self.op, self.right)


class SUnaryOp(SExpression):
	def __init__(self, op, operand):
		self.op = op
		self.operand = operand
		self._fields = ['operand']	
	
	def generate(self):
		yield "(%s (%s))" % (self.op, self.operand)

class SSubscript(SExpression):
	def __init__(self, value, index):
		self.value = value
		self.index = index
		self._fields = ['value', 'index']

	def generate(self):
		yield "%s[%s]" %(self.value, self.index)

class SPrint(SGenerable):
	def __init__(self,text,newline):
		self.text = text
		self.newline = newline
		self.done = False
		
	def __iter__(self):
		return self
	
	def next(self):
		if self.done:
			raise StopIteration
		else:
			self.done = True
			return self
		
class SReturnStatement(SGenerable):
	def __init__(self, retval):
		self.retval = retval
		self._fields = ['retval']
		self.done = False

	def __iter__(self):
		return self
	
	def next(self):
		if self.done:
			raise StopIteration
		else:
			self.done = True
			return self
		
	def generate(self):
		ret = 'return ' + str(self.retval)
		yield ret

class SAssign(SGenerable): #should this inherit from something else???
	def __init__(self, lvalue, rvalue):
		##??
		self.lvalue = lvalue
		self.rvalue= rvalue
		self._fields = ['lvalue', 'rvalue']
		self.done = False

	def __iter__(self):
		return self
	
	def next(self):
		if self.done:
			raise StopIteration
		else:
			self.done = True
			return self
		
	def generate(self):
		#need to correctly have left side? if new variable problematic
		lvalue = str(self.lvalue) ## this will need to be amended
		rvalue = str(self.rvalue)
		yield "%s = %s" % (lvalue, rvalue)	
		
		
#for some reason is not an iterable?? needs to be a sequence
class SCompare(SGenerable):
	def __init__(self, left,op,right):
		self.left = left
		self.op = op
		self.right = right
		self.done=False
		self._fields = ('left', 'op', 'right')
		
	def __iter__(self):	
		return self
	
	def next(self):
		if self.done:
			raise StopIteration
		else:
			self.done=True
			return self
		
class SIfConv(SGenerable):
	def __init__(self, test, body, orelse):
		self.test = test
		self.body = body
		self.orelse = orelse
		self.done= False

	def __iter__(self):
		return self
	
	def next(self):
		if self.done:
			raise StopIteration
		else:
			self.done = True
			return self
		
class SFor(SGenerable): #??
	def __init__(self, target, iter, body):
		self.target = target
		self.iter = iter
		self.body = body
		self.done= False

	def __iter__(self):
		return self
	
	def next(self):
		if self.done:
			raise StopIteration
		else:
			self.done = True
			return self
	
	def generate(self):
		pass

class SWhile(SGenerable):
	def __init__(self, test, body):
		self.test = test
		self.body = body
		self._fields = []
		self.done= False

	def __iter__(self):
		return self
	
	def next(self):
		if self.done:
			raise StopIteration
		else:
			self.done = True
			return self
	
	def generate(self):
		pass
	
if __name__ == '__main__':
	f= open('times4.py')
	rendered = f.read()
	print 'RENDERED:', rendered
	print 'DONE WITH RENDERED'
	node = ast.parse(rendered)
	str = ast.dump(node)
	code = codegen.to_source(node)
	print str
	print 'CODE:'
	print code
	print 'DONE WITH CODE'
	print 'heres num:', node.n

