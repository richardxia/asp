
import java.util.ArrayList;
import spark._
import SparkContext._
import javro.scala_arr
import line_count._

var DIM = 1

def bootstrap(data: Array[Array[String]]): Array[Array[String]] = {
	var bootstrap_vectors = new Array[Array[Array[String]]](data.length/this.DIM)

	for (i <- Range(0, data.length/this.DIM)){
		var store_arr = new Array[Array[String]](data.length)
		var count = 0
		for (j <- Range(i*this.DIM, (i+1)*this.DIM)){
			store_arr(count)=data(j)
			count+=1
		}			
		bootstrap_vectors(i) = store_arr
	}
	var bootstrap = new Array[Array[String]](data.length)
	var len = bootstrap_vectors.length
	var gen = new java.util.Random()
	
	for (i <- Range(0, data.length/this.DIM)){
		var store_arr = bootstrap_vectors(gen.nextInt(len))
		for (j <- Range(0, this.DIM)){
			bootstrap(i*DIM + j) = store_arr(j)
		}
	}
	return bootstrap
}

def subsample(data: Array[Array[String]], subsample_len_exp:Double): Array[Array[String]] = {
	
	var subsample_len = math.pow(data.size, subsample_len_exp).asInstanceOf[Int]
	                                                                        
	var subsample_indicies = new Array[Int](subsample_len)
	var gen = new java.util.Random()
	var len = data.size
	for (i <- Range(0, subsample_len)){
		subsample_indicies(i) = gen.nextInt(len/ this.DIM)
	}

	var subsample = new Array[Array[String]](subsample_len*this.DIM)
	var count = 0
	for (s <- Range(0, subsample_indicies.length)){
		var index = subsample_indicies(s)
		for (j <- Range((index*this.DIM).asInstanceOf[Int], ((index+1)*this.DIM).asInstanceOf[Int])){
			subsample(count) = data(j)
			count += 1
		}
	}
	return subsample
}

def readEmails(filename: String): Array[Array[String]]={
	import io._
	println("filename is:" + filename)
	var email_iter = Source.fromFile(filename).getLines()
	
	var emails = new Array[Array[String]](line_count.count(filename))
	var index = 0
    for (email <- email_iter){
    	var email_str_ar = email.split(" ")
    	emails(index) = email_str_ar
    	index +=1
    }
	return emails
}

//will there be more than one vector in the model ever ?
def parseModel(filename: String): Array[Array[Double]]={
	import io._
	var model_iter = Source.fromFile(filename).getLines()
	var lines_num = line_count.count(filename)
	var count = 1
    var concat_model = Array[String]()
    var classes_num = 0
    var features_num = 0
	for (line <- model_iter){
		if (count == 2){
			classes_num = Integer.parseInt(line.substring(0, line.indexOf(' ')))
		}
		if (count == 3){
			features_num = Integer.parseInt(line.substring(0, line.indexOf(' ')))
		}
		if (count == lines_num){
			concat_model = line.split(' ')
		}
		count +=1 
    }
	var models = new Array[Array[Double]](classes_num)
	var num = 0
	var class_ = 0
	var dim = 0
	var weight = 0.0
	for (i <- Range(0,classes_num)){
		var class_vec = new Array[Double](features_num)
		models(i) = class_vec
	}

	count = 0
	for (elem <- concat_model){
		if (count != 0 && count != 1 && elem != "#"){
			num = Integer.parseInt(elem.substring(0, elem.indexOf(':')))
			weight = java.lang.Double.parseDouble(elem.substring(elem.indexOf(':')+1, elem.length))
			class_ = (num-1)/features_num 
			dim = (num-1) % features_num
			models(class_ )(dim) = weight	
		}
		count +=1
	}
	/**
	println("concat")
	for (x <-concat_model){
		println(x)
	}
	println("adsfsadf")
	for (m<-models){
		println("model------------------------")
		for (x <- m){
			println(x)
		}
	}
	throw new Exception("caught")
	**/
    return models
}

def dot(vector1: Array[Double], vector2: Array[String]):Double={
	var total = 0.0
	var count = 1
	var email_elem = vector2(count)
	var count2 = 0
	var weight = 0.0
	var dim = 0
	for (elem <- vector1){
		dim = Integer.parseInt(email_elem.substring(0, email_elem.indexOf(':')))-1
		if (dim == count2){
			weight = java.lang.Double.parseDouble(email_elem.substring(email_elem.indexOf(':')+1, email_elem.length))
			total += elem*weight
			count +=1
			if (count != vector2.length){
				email_elem = vector2(count)
			}
		}		
		count2 +=1 
	}	      
	return total
}


def run(email_filename: String, model_filename:String, DIM: Int, 
			num_subsamples:Int, num_bootstraps:Int, subsample_len_exp:Double):Double={
	this.DIM = DIM
	var sc = new SparkContext(System.getenv("MASTER"), "Blb", "/root/spark", List(System.getenv("FILE_LOC")))
	//var sc = new SparkContext("local", "Blb", "/root/spark", List(System.getenv("FILE_LOC")))

	//var data = (new JAvroInter("out.avro", "data.avro")).returnStored[scala_arr[Double]](0)
	//val broadcastData = sc.broadcast(data.stored)	
	val bnum_bootstraps = sc.broadcast(num_bootstraps)
	val bsubsample_len_exp = sc.broadcast(subsample_len_exp)

	val emails = sc.broadcast(readEmails(email_filename))
	val model = sc.broadcast(parseModel(model_filename))

	var run_func = (x:Double)=>{		
		var funcs = new run_outer_data()		
		var subsamp = funcs.subsample(emails.value, bsubsample_len_exp.value)
		var bootstrap_estimates = new Array[Double](bnum_bootstraps.value)
		for (j <- Range(0, bnum_bootstraps.value)){
			var btstrap = funcs.bootstrap(subsamp)	
			bootstrap_estimates(j) = funcs.compute_estimate(btstrap, model.value)
		}
		funcs.reduce_bootstraps(bootstrap_estimates)
	}

	return average( sc.parallelize(new Array[Double](num_subsamples)).map(run_func).collect() ) 

}


/**
what will have to be done to run classifier?
change call to compute estimate? hard-code in, ehh

change hard-coding everywhere previously from array doubles
combine emails and tags perhaps, and hardcode model argument in?
		OR, DO SOMETHING LIKE I DO WITH AVRO CALL
		am going to have to add function type decls in...
		could then determine type/number of compute_estimate args..EASY!
		
		...but how to determine where these args come from/how to call?
				any easy way to know this?
		could just read model from inside compute_estimate?

		for blb functions, could make convertPyAst_ScalaAst instance
			and feed in to that the function types (shouldn't be too hard)
		
		should create new AST Py/scala node type for function type decs
		could call compute_estimate with the iterator instead ?

where will all these emails be stored? 
		distributed already? tough to do,
				but probably have to to not be terribly slow...

so each node will store some of this data?

in run, will need to get model vector from file?
how did they convert type of compute estimate to c? hardcode in?

still need to write dot function in scala...

probably would be better for readEmails just to return StringIterator
run() should be called with perhaps email and model filenames?
		hardcoding for this example though..
		.. eh or could do something like whatever extra args run is called with
				they are passed into compute_estimate
**/




/**

-6.8862271
2* -7.1428566
-7.1428566
-7.1428566
2*10.479042



-26.0581674

**/













