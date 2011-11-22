"""Represents a particular execution order trace of a specializer.

Ideally, there will be a function that automatically forces a Python 
execution to match a given trace.
"""
class AspTrace(object):
    def __init__(self):
        self.logs = []

    def parse(text):
        for line in text.splitlines():
            # FIXME: This is ghetto, do smarter parsing
            if line.find('('):
                raise Exception, "Bad format"
            line = line[line.find('(')+1:-1]
            value, iteration, target = line.split(',')
            self.logs.append(AspWrite())

    def parse_file(filename):
        f = open(filename)
        parse(f.read())
        f.close()

class AspWrite(object):
    def __init__(self, value, iteration, target):
        self.value = value
        self.iteration = iteration
        self.target = target
