



TYPE_DECS = ('TYPE_DECS', 
             #(func_name, [input types], return type)
             ['double', [('array', 'int')], ('array', 'int')])

def double(arr):    
    size = arr.size()    
    i = 0
    while (i < size):
        arr.set(i,2 * arr.get(i))
        i += 1
    return arr

