
//import avroInter._
import org.apache.avro.generic.GenericData
import org.apache.avro.JAvroInter

object double_using_scala {
  def double(arr: GenericData.Array[Double]): GenericData.Array[Double] = {
    var len = arr.size()
    for (i<-0 until len){
        arr.set(i, arr.get(i)*2.0) 
    }
    return arr
  }
  
  def main(args: Array[String]) {    
        
    var s = new JAvroInter("results.avro", "args.avro")          
    var genArr = s.returnInput[GenericData.Array[Double]](0)      
     
    genArr = double(genArr)          

    var results = new Array[Object](1)
    results(0) = genArr
    s.writeAvroFile(results)          
  }
}       
