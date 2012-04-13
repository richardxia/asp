



TYPE_DECS = ('TYPE_DECS', 
             #(func_name, [input types], return type)
             ['double', [('array', 'int')], ('array', 'int')])

def double(arr):    
    size = arr.size()    
    i = 0
    while (i < size):
        arr[i] = 2 * arr[i]
        i += 1
    return arr

