
import asp.jit.asp_module as asp_module
import avroInter.avro_backend as avro_backend    
import asp.codegen.ast_tools as ast_tools
import asp.codegen.codegenScala as codegenScala
import ast

def combine(blb_funcs):
    parent = (open('blb_core.scala')).read()  
    return parent + blb_funcs
    

class BLB:   
    def blb_py2scala(self, data, num_subsamples=25, num_bootstraps=50, subsample_len_exp=.5):      
        mod = asp_module.ASPModule(use_scala=True)      
        f = open('blb_funcs.py')
        rendered_py = f.read()     
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
        funcs_ast_py = ast.parse(rendered_py)          
        funcs_ast_scala = ast_tools.ConvertPyAST_ScalaAST().visit(funcs_ast_py)    
        rendered_scala = codegenScala.to_source(funcs_ast_scala)     
        
        rendered_scala = combine(rendered_scala)
        
        # the first arg below specifies the main function in the set of input functions, 
        # the one to be called first by main with the input args
        rendered = avro_backend.generate_scala_object("run","",rendered_scala)  
        
        #NOTE: must append outer to function name above to get the classname
        
        mod.add_function("run_outer", rendered, backend = "scala")   
        
        f = open('scala_lib.scala')        
        rendered_scala_lib = f.read()
        mod.add_function('lib', rendered_scala_lib, backend="scala")
        
        #print 'RENDERED PY IS:', rendered_py         
        #print '-------------------------------------------------------------'      
        #print 'RENDERED SCALA IS:', rendered_scala      
        #print '-------------------------------------------------------------'  
        print
        print 'FULLY RENDERED SCALA WITH MAIN IS:', rendered
        print '-------------------------------------------------------------'
        return mod.run_outer(data, num_subsamples, num_bootstraps, subsample_len_exp)

"""
data = [i*1.0 for i in range(500)]
res = BLB().blb_py2scala(data, 25, 50, .5)
print 'FINAL RESULT IS:', res
"""