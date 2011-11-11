
import json
from avro import schema, datafile, io

"""
PYTHON            JSON        AVRO
dict            object        record
list,tuple        array        array
str,unicode        string        string
int,long,float    number        float,double?
True            true            boolean
False            false            boolean
None            null            null

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
        raise Error("Unrecognized type")
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
        if arg != args[-1]:
            entry+= ','
        schema = schema + entry
        count+=1
    close = """
    ]
}"""
    schema = schema + close
    return schema

    
def write_avro_file(args, outfile='args.avro'):
    SCHEMA = schema.parse(makeSchema(args))
    rec_writer = io.DatumWriter(SCHEMA)   
    OUTFILE_NAME = outfile
    df_writer = datafile.DataFileWriter(open(OUTFILE_NAME,'wb'), rec_writer, 
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
    
def tupleToList(input):
    output = list(input)
    for i in range(len(output)):
        if type(output[i]) == tuple:
            output[i] = list(output[i])
    return output

def read_avro_file(filename='results.avro'):
    rec_reader = io.DatumReader()
    OUTFILE_NAME = filename
    df_reader = datafile.DataFileReader(open(OUTFILE_NAME), rec_reader)
    results = []
    for record in df_reader:
        size = record['size']
        for i in range(size):
            i = i+1
            arg = record["arg%s"%(i)]
            results.append(arg)
    return results

if __name__ == '__main__':
    OUTFILE_NAME = 'args.avro' 
    args = [ 1,[1,2,3],"asfdsf"]#,[[1,2,4], [2,3,5]],[1,2,3,4], "asdf",[1,3], [1,2,34]]
    write_avro_file(args)
    read_avro_file()
