
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.apache.avro.generic.GenericData

class SAvroInter(OUTPUT_FILE_NAME:String, INPUT_FILE_NAME:String)
	 extends org.apache.avro.JAvroInter(OUTPUT_FILE_NAME, INPUT_FILE_NAME){
		
	// converts singly nested arrays to java lists 
	def convertArr[T](arr :Array[T]) : java.util.List[T]= {
		return bufferAsJavaList(ListBuffer(arr.iterator.toList:_*))
	}

	//any conversions that may need to be done in scala
	 def convertClasses(){
		/**
		for (i <-0 until inputs.length){
			var inputClass = inputs(i).getClass()
			if (inputClass.isArray()){
				//code to convert objects
				
				var innerClass = (inputs(i).asInstanceOf[Array[Object]]).getClass()
			}
		}
		**/
	}
}

object savTester{
	
	def main (args: Array[String]){
		var s = new SAvroInter("results.avro", "args.avro")
		//println(s.returnInput[Int](0))

		//var u = new org.apache.avro.util.Utf8("asdf")
		//println(u)

		
		//var arr = new Array[Int](3);
		//println(arr.getClass())
		
		var i2 = s.returnInput[org.apache.avro.generic.GenericData.Array[Int]](0)
		
		println("here's 2: " + i2)
		println(i2.getClass())
		println(i2.get(1).getClass())
		println((i2.get(1)))
		
		//s.printInputs
	}
}
