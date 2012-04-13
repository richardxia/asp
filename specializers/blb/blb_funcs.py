
import random

TYPE_DECS = ('TYPE_DECS', 
             #(func_name, [input types], return type)
             ['compute_estimate', [('array', 'double')], 'double'],             
             ['reduce_bootstraps', [('array', 'double')], 'double'],
             ['average', [('array', 'double')], 'double']
            )


DIM = 1

def compute_estimate(data):
    avg = 0.0
    if len(data)==0: return avg
    else:
        for i in range(len(data)):
            avg += data[i]
        return avg / len(data)
    #return stddev(data)

def reduce_bootstraps(data):
    avg = 0.0
    if len(data)==0: return avg
    else:
        for i in range(len(data)):
            avg += data[i]
        return avg / len(data)
    #return mean(data)

def average(data):
    avg = 0.0
    if len(data)==0: return avg
    else:
        for i in range(len(data)):
            avg += data[i]
        return avg / len(data)

