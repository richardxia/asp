import os
import os.path
import subprocess
from avroInter.PyAvroInter import *
import sys

class ScalaFunction:
    def __init__(self, classname, source_dir):
        self.classname = classname
        self.source_dir = source_dir                               
    
    def find_close(self,str):
        index = len(str)-1
        print 'index is:',index
        print 'str len is:', len(str)
        char = str[index]
        
        while (char!=']'):
            index -=1
            char = str[index]
        return index
 
    
    def __call__(self, *args, **kwargs):
                   
        write_avro_file(args, 'args.avro')        
        p1 = subprocess.Popen(['scala', '-classpath', self.source_dir + ':../../avroInter', self.classname], stdin=None, stdout=subprocess.PIPE)
        p1.wait()
                 
        if p1.returncode != 0:
            raise Exception("Bad return code")
            
        read_avro_file('results.avro')
        
        os.remove('args.avro')
        os.remove('results.avro')
        
        #cmd = '/bin/bash -c "cat %s | python ../../avroInter/PyAvroInter.py write | python ../../avroInter/PyAvroInter.py read"'%(args_file.name) 
        
        #cmd = '/bin/bash -c "pr -t %s | python ../../avroInter/PyAvroInter.py write "' %(args_file.name)
        #cmd = '/bin/bash -c "pr -t %s | python ../../avroInter/PyAvroInter.py write | python ../../avroInter/PyAvroInter.py read"' %(args_file.name)       
        #cmd = '/bin/bash -c "pr -t %s | python ../../avroInter/PyAvroInter.py write | scala -classpath %s:../../avroInter %s ' %(args_file.name, self.source_dir,self.classname)
        #cmd = '/bin/bash -c "cat %s | python ../../avroInter/PyAvroInter.py write | scala -classpath %s:../../avroInter %s | python ../../avroInter/PyAvroInter.py read"' %(args_file.name, self.source_dir,self.classname)
        #cmd = '/bin/bash -c "python ../../avroInter/PyAvroInter.py write %s | scala -classpath %s:../../avroInter %s | python ../../avroInter/PyAvroInter.py read"' %(args_file.name, str_args,self.source_dir,self.classname)
        
        """
        p = pexpect.spawn(cmd, timeout=200, maxread=40000,searchwindowsize=4000)
        p.expect(pexpect.EOF)
        p.close()
        
        if p.exitstatus == 1:
            raise Exception("BAD RETURN CODE:\n" + str(p.before))
                
        """
        #print "P BEFORE:", p.before
        #str_res = p.before[0:self.find_close(p.before)+1]
        #print "STRING RESULT:", str_res
        #result = eval(str_res)
        #print 'RESULT:', result

class PseudoModule:
    '''Pretends to be a Python module that contains the generated functions.'''
    def __init__(self):
        self.__dict__["__special_functions"] = {}

    def __getattr__(self, name):
        if name in self.__dict__["__special_functions"].keys():
            return self.__dict__["__special_functions"][name]
        else:
            raise Error

    def __setattr__(self, name, value):
        self.__dict__["__special_functions"][name] = value

class ScalaModule:
    def __init__(self):
        self.mod_body = []
        self.init_body = []

    def add_to_init(self, body):
        self.init_body.extend([body])

    def add_function(self):
        # This is only for already compiled functions, I think
        pass

    def add_to_module(self, body):
        self.mod_body.extend(body)

    def add_to_preamble(self):
        pass

    def generate(self):
        s = ""
        for line in self.mod_body:
            if type(line) != str:
                raise Error("Not a string")
            s += line
        return s

    def compile(self, toolchain, debug=True, cache_dir=None):
        if cache_dir is None:
            import tempfile
            cache_dir = tempfile.gettempdir()
        else: 
            if not os.path.isdir(cache_dir):
                os.makedirs(cache_dir)

        source_string = self.generate()
        hex_checksum = self.calculate_hex_checksum(source_string)
        mod_cache_dir = os.path.join(cache_dir, hex_checksum)
        # Should we assume that if the directory exists, then we don't need to
        # recompile?
        if not os.path.isdir(mod_cache_dir):
            os.makedirs(mod_cache_dir)
            filepath = os.path.join(mod_cache_dir, "asp_tmp.scala")

            source = open(filepath, 'w')
            source.write(source_string)
            source.close()
            result = os.system("scalac -d %s -cp %s %s" % (mod_cache_dir, "../../avroInter", filepath))    
            #result = os.system("scalac -d %s %s" % (mod_cache_dir, filepath))
            
            os.remove(filepath)
            if result != 0:
                os.system("rm -rf " +  mod_cache_dir)
                raise Exception("Could not compile")
               
        mod = PseudoModule()
        for fname in self.init_body:
            self.func = ScalaFunction(fname, mod_cache_dir)
            setattr(mod, fname, self.func)
        return mod

    # Method borrowed from codepy.jit
    def calculate_hex_checksum(self, source_string):
        try:
            import hashlib
            checksum = hashlib.md5()
        except ImportError:
            # for Python << 2.5
            import md5
            checksum = md5.new()

        checksum.update(source_string)
        #checksum.update(str(toolchain.abi_id()))
        return checksum.hexdigest()


class ScalaToolchain:
    pass
