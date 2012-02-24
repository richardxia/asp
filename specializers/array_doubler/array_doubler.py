# really dumb example of using templates w/asp

#import avro_backend

class ArrayDoubler(object):
    
    def __init__(self):
        self.pure_python = True

    def double_using_template(self, arr):
        import asp.codegen.templating.template as template
        mytemplate = template.Template(filename="templates/double_template.mako", disable_unicode=True)
        rendered = mytemplate.render(num_items=len(arr))

        import asp.jit.asp_module as asp_module
        mod = asp_module.ASPModule()
        # remember, must specify function name when using a string
        mod.add_function("double_in_c", rendered)
        return mod.double_in_c(arr)


    def double_using_scala(self, arr):
        import asp.jit.asp_module as asp_module
        import avroInter.avro_backend as avro_backend
        mod = asp_module.ASPModule(use_scala=True)
                        
        rendered = avro_backend.generate_scala_object("double","func1.scala")
        """
        f = open("func.scala")
        rendered = f.read()
        f.close()
        """
        #mainfunc name needs to be the same as the name for the added function below
        mod.add_function("double", rendered, backend="scala")
        return mod.double(arr, 2, "asdfasdf")


    def double_py2scala(self,arr):
        import asp.jit.asp_module as asp_module
        import avroInter.avro_backend as avro_backend    
        import asp.codegen.ast_tools as ast_tools
        import asp.codegen.codegenScala as codegenScala
        import ast
        
        mod = asp_module.ASPModule(use_scala=True)      
        f = open('func.py')
        rendered_py = f.read()     
        
        func_ast_py = ast.parse(rendered_py)               
        func_ast_scala = ast_tools.ConvertPyAST_ScalaAST().visit(func_ast_py)               
        rendered_scala = codegenScala.to_source(func_ast_scala)        
        rendered = avro_backend.generate_scala_object("double","",rendered_scala)        
        mod.add_function("double", rendered, backend="scala")   
        
        print 'RENDERED PY IS:', rendered_py         
        print '-------------------------------------------------------------'      
        print 'RENDERED SCALA IS:', rendered_scala      
        print '-------------------------------------------------------------'  
        print 'FULLY RENDERED SCALA WITH MAIN IS:', rendered
        print '-------------------------------------------------------------'
         
        return mod.double(arr)
        
    def double(self, arr):
        arr2 = [1,2,3]               
        return map (lambda x: x*2, arr)
        

