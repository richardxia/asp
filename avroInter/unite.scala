import org.apache.avro.generic.GenericData

object unite {

	def main(args: Array[String]){
		var s = new org.apache.avro.JAvroInter("results.avro", "args.avro")
		var array = s.returnInput[GenericData.Array[Int]](1)

		// how to convert garray to specific kind of Array???

		var iarr = array.asInstanceOf[java.util.AbstractList[Int]].toArray[Int]_
		println(iarr.getClass())
		//println(iarr(0))

		var results = new Array[Object](4)

		results(0) = new Integer(3)
		results(1) = "asdfasdf"
		var arr = new Array[Int](2)
		arr(0) = 3
		arr(1) = 23
		results(2) = s.convertArr[Int](arr)

		var arr2 = new Array[Array[String]](2)
		arr2(0) = new Array[String](2)
		arr2(1) = new Array[String](2)
		
		for (i<-0 until arr2.length){
			//arr2(i) = convert[String](arr2(i))
		}
		//results(3) =s.convertArr[Array[String]](arr2)//bufferAsJavaList(ListBuffer(arr2.iterator.toList:_*))
		results(3) = array
		//println(bufferAsJavaList(ListBuffer(List(1,2,3):_*)))
		println("done")
		s.writeAvroFile(results)
		
		var reader = new org.apache.avro.JAvroInter("adsfsdf.avro", "results/results.avro")
		reader.printInputs
	}

	// need to make this work for recursive lists?
	/**
	def toList[T](arr :Array[T]) : java.util.List[T]={
		if (arr.getClass.getComponentType.isArray){
			return convert[T](arr, 1)		
		}
		else {
			return convert[T](arr)
		}
	}

	def convertArr[T](arr :Array[T] ) : java.util.List[T]={
		/**
		if (depth > 0){
			for (i <- 0 until arr.length){
				var c = arr(i).getClass.getComponentType
				arr(i) = convert[c](arr(i), depth-1)
			}
		}
		**/
		return bufferAsJavaList(ListBuffer(arr.iterator.toList:_*))
	}

	**/
	def arrayDouble(arr :Array[Int]) : Array[Int]= {
		return arr map {_ * 2 }	
	}
}

