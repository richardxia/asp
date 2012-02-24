
import sys

#must add these files to python classpath
from avro import schema, datafile, io
from cStringIO import StringIO

"""
PYTHON            JSON        AVRO
dict             object        record
list,tuple       array        array
str,unicode      string        string
int,long,float   number        float,double?
True             true            boolean
False            false            boolean
None             null            null

JSON    AVRO
null    null
boolean    boolean
integer    int,long
number    float,double
string    string
object    record
string    enum
array    array
object    map
"""   

"""
TO NOTE:
1) lists can only be of one type
2) tuples are converted to lists
"""

stored = []
to_write = []

def getAvroType(pyObject):
    t = type(pyObject)
    if t == dict:
        return '"record"'
    elif t == list or t == tuple:
        if pyObject:
            listType = getAvroType(pyObject[0])
        else:
            #list is empty...
            listType = '"int"'
        entry = """{    "type":"array", "items": %s    }"""%(listType)
        return entry
    elif t == str:
        return '"string"'
    elif t == int:
        return '"int"'
    elif t == long:
        return '"long"'
    elif t == float:
        return '"double"'
    elif t == bool:
        return '"boolean"'
    elif t == type(None):
        return '"null"'
    else:
        raise Exception("Unrecognized type")
    return entry
        

def makeSchema(args):
    schema = """{
    "type": "record",
    "name": "args",
    "namespace": "SCALAMODULE",
    "fields": ["""
    count = 1
    size = """
        { "name": "size"    , "type": "int"    }"""
    if args:
        size += ","            
    schema = schema +size
    for arg in args:
        t = getAvroType(arg)
        entry = """
        {    "name": "arg%s"    , "type": %s    }"""%(count,t)
        if count != len(args):
            entry+= ','
        schema = schema + entry
        count+=1
    close = """
    ]
}"""
    schema = schema + close
    return schema

    
def write_avro_file(args, outsource='args.avro'):
    SCHEMA = schema.parse(makeSchema(args))
    rec_writer = io.DatumWriter(SCHEMA)   
        
    if outsource == sys.stdout:
        df_writer = datafile.DataFileWriter(sys.stdout, rec_writer, 
                                        writers_schema = SCHEMA, codec = 'deflate')
    
    else:
        df_writer = datafile.DataFileWriter(open(outsource,'wb'), rec_writer, 
                                        writers_schema = SCHEMA, codec = 'deflate')
    data = {}
    count = 1
    data['size'] = len(args)
    for arg in args:
        if type(arg) == tuple:
            arg = tupleToList(arg)
        data["arg%s"%(count)] = arg
        count +=1
    df_writer.append(data)
    df_writer.close()

#this function reads the specified avro file and stores the data in the global list stored
def read_avro_file(insource='results.avro'):
    rec_reader = io.DatumReader()
    if insource == sys.stdin:      
        #DataFileReader cannot read from streams like sys.stdin because it calls
        #seek so a temp file is necessary
        
        input = sys.stdin.read()
        temp_file = StringIO(input)

        df_reader = datafile.DataFileReader(temp_file, rec_reader)
    else:
        df_reader = datafile.DataFileReader(open(insource), rec_reader)
    del stored[:]
    for record in df_reader:
        size = record['size']
        for i in range(size):
            i = i+1
            arg = record["arg%s"%(i)]
            #print arg
            stored.append(arg)
    return stored[0]

def return_stored(index):
    if stored:
        return stored[index]
    else:
        read_avro_file()
        return stored[index]
    
def return_stored():
    if stored:
        return stored
    else:
        read_avro_file()
        return stored

def set_args_to_write(args):
    del to_write[:]
    for a in args:
        to_write.append(a)
        
def tupleToList(input):
    output = list(input)
    for i in range(len(output)):
        if type(output[i]) == tuple:
            output[i] = list(output[i])
    return output
        
if __name__ == '__main__': 
    args = sys.argv   
    inputs = [1,2,3,4]
    write_avro_file(inputs)
    print read_avro_file()
    """
    if len(args) >1:
        if args[1] == 'write':
            #inputs = eval(args[2].replace('/', "'"))   
            inputs = sys.stdin.read().replace('/', "'")
            inputs = eval(inputs)
            write_avro_file(inputs, sys.stdout)

        elif args[1] == 'read':
            read_avro_file('results.avro')
        else:
            raise Exception("Unknown first arg type. Valid options are 'read' or 'write'")
    else:
        raise Exception("More arguments required")
    """
