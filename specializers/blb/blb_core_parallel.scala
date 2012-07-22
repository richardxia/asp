
import java.util.ArrayList;
import spark._
import SparkContext._
import javro.scala_arr
import line_count._

def formatEmail(vector: Array[String]): Email={

	var em = new Email()
	em.vec_indices = new Array[Int](vector.length-1)
	em.vec_weights = new Array[Int](vector.length-1)

	var first = true
	var num = 0
	var weight = 0
	var count = 0
	for (elem <- vector){
		if (first){
			em.tag = Integer.parseInt(elem)
			first = false
		}
		else {
			num = Integer.parseInt(elem.substring(0, elem.indexOf(':')))
			weight = Integer.parseInt(elem.substring(elem.indexOf(':')+1, elem.length))
			em.vec_indices(count) = num
			em.vec_weights(count) = weight
			count += 1
		}
	}
	return em
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
    return models
}

def custom_dot(model:Array[Double], email:Email):Double={
		var email_indices = email.get_vec_indices()
		var email_weights = email.get_vec_weights()
		var total =0.0
		var index = 0
		var weight = 0
		for (i <- Range(0, email_indices.length)){
			index = email_indices(i)
			weight = email_weights(i)
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
    
    val num_emails = distEmails.count().asInstanceOf[Int]
    val bnum_emails = sc.broadcast(num_emails)
    val rand_prob = sc.broadcast(math.pow(num_emails, subsample_len_exp)/num_emails)

    val modelFile = sc.textFile(model_filename)
    //val modelFile = sc.textFile("s3://AKIAJVLVU3XLP4GLMFEA:xZtDvTF5z0QYx5pZ8gI9KoSpcPHfKarUiNXDKGhy@largeModel/")
    val models = sc.broadcast(parseModel(modelFile.collect()))


    var subsamps = distEmails.flatMap(email =>{
    	val gen = new java.util.Random()
    	var subsamp_count = 0
    	var outputs = List[(Int, Email)]()
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

    // --> (Subsamp Id, subsample) --> (Int, Seq[Email])

    //assuming DIM=1 in map

    var subsamps_out = subsamps.flatMap(subsamp => {
    				//List (subsamp id, subsample) --> (subsample#, Seq(emails))
    	var outputs = List[(Int, Seq[Email])]()
    	for (i <- Range(0, bnum_bootstraps.value)){
    		outputs ::= subsamp
    	}
    	outputs
    }).map(subsamp => {
    	val funcs = new run_outer_data()
    	val subsamp_id = subsamp._1
    	var subsamp_vec = subsamp._2
  
    	val gen = new java.util.Random()
    	var email =""
    	var estimate = 0.0
    	var len = subsamp_vec.size

    	var subsamp_weights = new Array[Int](subsamp_vec.size)

    	for (i <- Range(0, bnum_emails.value)){
    		//subsamp_weights(gen.nextInt(len)) += 1
    		subsamp_vec(gen.nextInt(len)).weight += 1
    	}

    	/**
    	var count = 0
    					// (subsamp id, subsamp[(email count in resamp, email)])
    	var weighted_subsamp_vec = List[(Int, Array[(Int, Int)])]()              
    	for (email <- subsamp_vec){
    		weighted_subsamp_vec ::= (subsamp_weights(count), email)  
    		count += 1 
    	}
    	**/
    	(subsamp_id, funcs.compute_estimate(subsamp_vec.asInstanceOf[List[Email]], models.value))

    }).groupByKey().map(bootstrap_estimates =>{
    	val funcs = new run_outer_data()
    	funcs.reduce_bootstraps(bootstrap_estimates._2.asInstanceOf[List[Double]])
    }).collect()
    
    return average(subsamps_out)
}


