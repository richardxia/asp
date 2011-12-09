  
import org.apache.avro.generic.GenericData

def double(arr: GenericData.Array[Double], num: Int, str: String): GenericData.Array[Double] = {
    var len = arr.size()
    for (i<-0 until len){
        arr.set(i, arr.get(i)*2.0) 
    }
    return arr
  }
  