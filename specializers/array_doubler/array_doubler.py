# really dumb example of using templates w/asp

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
        mod = asp_module.ASPModule(use_scala=True)
                        
        f = open("func.scala")
        rendered = f.read()
        f.close()
        
        mod.add_function("double_using_scala", rendered, backend="scala")
        return mod.double_using_scala(arr)

    def double(self, arr):
        return map (lambda x: x*2, arr)
        

