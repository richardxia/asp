
import java.util.ArrayList;
import spark._
import SparkContext._
import javro.scala_arr
import line_count._

def formatEmail(vector: Array[String]): Array[(Int,Int)]={
	var out_vec = new Array[(Int, Int)](vector.length)
	var first = true
	var num = 0
	var weight = 0
	var count = 0
	for (elem <- vector){
		if (first){
			out_vec(0) = (-1, Integer.parseInt(elem))
			first = false
		}
		else {
			num = Integer.parseInt(elem.substring(0, elem.indexOf(':')))
			weight = Integer.parseInt(elem.substring(elem.indexOf(':')+1, elem.length))
			out_vec(count) = (num, weight)
		}
		count +=1
	}
	return out_vec
}

def parseModel(lines: Array[String]): Array[Array[Double]]={
	import io._
	var lines_num = lines.size
	var count = 1
    var concat_model = Array[String]()
    var classes_num = 0
    var features_num = 0
	for (line <- lines){
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

def custom_dot(model:Array[Double], email:Array[(Int,Int)]):Double={
		var total =0.0
		var index = 0
		var weight = 0
		for (pair <- email){
			index = pair._1
			weight = pair._2
			total += model(index-1) * weight
		}
		return total
}

def run(email_filename: String, model_filename:String, DIM: Int, 
			num_subsamples:Int, num_bootstraps:Int, subsample_len_exp:Double):Double={
	
	val sc = new SparkContext(System.getenv("MASTER"), "Blb", "/root/spark", List(System.getenv("FILE_LOC")))
	val bnum_bootstraps = sc.broadcast(num_bootstraps)
	val bsubsample_len_exp = sc.broadcast(subsample_len_exp)
	val bnum_subsamples = sc.broadcast(num_subsamples)

    var distEmails = sc.sequenceFile[Int, String](email_filename)
    //var distEmails = sc.sequenceFile[Int, String]("s3://AKIAJVLVU3XLP4GLMFEA:xZtDvTF5z0QYx5pZ8gI9KoSpcPHfKarUiNXDKGhy@largeEmail/")
    
    val num_emails = distEmails.count()
    val bnum_emails = sc.broadcast(num_emails)
    val rand_prob = sc.broadcast(math.pow(num_emails, subsample_len_exp)/num_emails)

    val modelFile = sc.textFile(model_filename)
    //val modelFile = sc.textFile("s3://AKIAJVLVU3XLP4GLMFEA:xZtDvTF5z0QYx5pZ8gI9KoSpcPHfKarUiNXDKGhy@largeModel/")
    val models = sc.broadcast(parseModel(modelFile.collect()))


    var subsamps = distEmails.flatMap(email =>{
    	val gen = new java.util.Random()
    	var subsamp_count = 0
    	var outputs = List[(Int, Array[(Int,Int)]()
    	var prob =0.0
    	val funcs = new run_outer_data()

    	for (i <- Range(0, bnum_subsamples.value)){
    		prob = gen.nextDouble()
    		if (prob < rand_prob.value){
    			outputs ::= (subsamp_count, funcs.formatEmail(email._2.split(" ")))
    		}
    		subsamp_count += 1
    	}
    	outputs  	
    }).groupByKey()

    // --> (Subsamp Id, subsample) --> (Int, Seq[Array[(Int, Int)]])

    //assuming DIM=1 in map

    var subsamps_out = subsamps.flatMap(subsamp => {
    				//List (subsamp id, subsample) --> (subsample#, Seq(emails))
    	val outputs = List[(Int, List[Array[(Int,Int)]])]()
    	for (i <- Range(0, bnum_boostraps)){
    		outputs ::= subsamp
    	}
    	outputs
    }).map(subsamp => {
    	val funcs = new run_outer_data()
    	val subsamp_id = subsamp._1
    	val subsamp_vec = subsamp._2

				// (subsamp id, subsamp[(email count in resamp, email)])
    	
    	var weighted_subsamp_vec = List[(Int, Array[(Int, Int)])]()              

    	for (email <- subsamp_vec){
    		weighted_subsamp_vec ::= (0, email)    		
    	}

    	val gen = new java.util.Random()
    	var email =""
    	var estimate = 0.0
    	var len = subsamp_vec.size

    	for (i <- Range(0, bnum_emails.value)){
    		weighted_subsamp_vec(gen.nextInt(len))._1 += 1
    	}
    	
    	(key, funcs.compute_estimate(weighted_subsamp_vec, models.value))
    }).groupByKey().map(bootstrap_estimates =>{
    	val funcs new run_outer_data()
    	funcs.reduce_bootstraps(bootstrap_estimates)
    }).collect()
    
    return average(subsamp_outs)
}
