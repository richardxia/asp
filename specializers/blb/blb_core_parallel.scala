
import java.util.ArrayList;
import spark._
import SparkContext._
import javro.scala_arr
import line_count._

var DIM = 1
var features_num = 0

def bootstrap(data: Array[Array[Int]]): Array[Array[Int]] = {
	var bootstrap_vectors = new Array[Array[Array[Int]]](data.length/this.DIM)

	for (i <- Range(0, data.length/this.DIM)){
		var store_arr = new Array[Array[Int]](data.length)
		var count = 0
		for (j <- Range(i*this.DIM, (i+1)*this.DIM)){
			store_arr(count)=data(j)
			count+=1
		}			
		bootstrap_vectors(i) = store_arr
	}
	var bootstrap = new Array[Array[Int]](data.length)
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

def subsample(data: Array[Int], subsample_len_exp:Double): Array[Int] = {
	
	var subsample_len = math.pow(data.size, subsample_len_exp).asInstanceOf[Int]
	                                                                        
	var subsample_indicies = new Array[Int](subsample_len)
	var gen = new java.util.Random()
	var len = data.size
	for (i <- Range(0, subsample_len)){
		subsample_indicies(i) = gen.nextInt(len/ this.DIM)
	}

	var subsample = new Array[Int](subsample_len*this.DIM)
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

def unflatten_vec(vector: Array[String], length:Int):Array[Int]={
	var full_vec = new Array[Int](length)
	var num = 0
	var weight = 0
	var first = true
	for (elem <- vector){
		if (first){
			full_vec(0) = Integer.parseInt(elem)
			println("just set 0 to:" + full_vec(0))
			first = false
		} 
		else{
			num = Integer.parseInt(elem.substring(0, elem.indexOf(':')))
			weight = Integer.parseInt(elem.substring(elem.indexOf(':')+1, elem.length))
			full_vec(num) = weight
		}
	}
	return full_vec
}

def readEmails(filename:String, selector: Array[Int]): Array[Array[Int]]={
	import io.Source
	import scala.util.Sorting
	Sorting.quickSort(selector)
	var email_iter = Source.fromFile(filename).getLines()
	var emails = new Array[Array[Int]](selector.length)
	var line_count = 0
	var selector_count = 0
	var current = selector(0)
	for (email <- email_iter){
		if (line_count == current){
			emails(selector_count) = unflatten_vec(email.split(" "), this.features_num+1)
			selector_count +=1
			if (selector_count < selector.length){
				current = selector(selector_count)
				while (current == line_count){
					emails(selector_count) = unflatten_vec(email.split(" "), this.features_num+1)
					selector_count += 1
					if (selector_count < selector.length){current = selector(selector_count)}
					else{current= -1}
				}
			}
		}
		line_count +=1 
	}
	return emails
}


def parseModel(filename: String): Array[Array[Double]]={
	import io._
	var model_iter = Source.fromFile(filename).getLines()
	var lines_num = line_count.count(filename)
	var count = 1
    var concat_model = Array[String]()
    var classes_num = 0
    //var features_num = 0
	for (line <- model_iter){
		if (count == 2){
			classes_num = Integer.parseInt(line.substring(0, line.indexOf(' ')))
		}
		if (count == 3){
			this.features_num = Integer.parseInt(line.substring(0, line.indexOf(' ')))
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
		var class_vec = new Array[Double](this.features_num)
		models(i) = class_vec
	}

	count = 0
	for (elem <- concat_model){
		if (count != 0 && count != 1 && elem != "#"){
			num = Integer.parseInt(elem.substring(0, elem.indexOf(':')))
			weight = java.lang.Double.parseDouble(elem.substring(elem.indexOf(':')+1, elem.length))
			class_ = (num-1)/this.features_num 
			dim = (num-1) % this.features_num
			models(class_ )(dim) = weight	
		}
		count +=1
	}
    return models
}

def dot(vector1:Array[Double], vector2:Array[Int]):Double={
		var total =0.0
		for (i <- Range(0, vector1.length)){
			total += vector1(i) * vector2(i+1)
		}
		return total
}


def run(email_filename: String, model_filename:String, DIM: Int, 
			num_subsamples:Int, num_bootstraps:Int, subsample_len_exp:Double):Double={
	this.DIM = DIM
	
	var sc = new SparkContext(System.getenv("MASTER"), "Blb", "/root/spark", List(System.getenv("FILE_LOC")))

	val bnum_bootstraps = sc.broadcast(num_bootstraps)
	val bsubsample_len_exp = sc.broadcast(subsample_len_exp)
	val b_email_file = sc.broadcast(email_filename)
	val b_model_file = sc.broadcast(model_filename)
	
	var run_func = (x:Double)=>{		
		var funcs = new run_outer_data()	
		var models = funcs.parseModel(b_model_file.value)

		var num_emails = line_count.count(b_email_file.value)
		var subsamp_arr = new Array[Int](num_emails)
		for (i <- Range(0,num_emails)){
			subsamp_arr(i) = i
		}

		var subsamp = funcs.subsample(subsamp_arr, bsubsample_len_exp.value)		
		var email_subsamp = funcs.readEmails(b_email_file.value, subsamp)

		var bootstrap_estimates = new Array[Double](bnum_bootstraps.value)
		for (j <- Range(0, bnum_bootstraps.value)){
			var btstrap = funcs.bootstrap(email_subsamp)	
			bootstrap_estimates(j) = funcs.compute_estimate(btstrap, models)
		}
		funcs.reduce_bootstraps(bootstrap_estimates)
	}

	return average( sc.parallelize(new Array[Double](num_subsamples)).map(run_func).collect() ) 

}


