
import java.util.ArrayList;
import spark._
import SparkContext._
import javro.scala_arr
import javro.JAvroInter
import line_count._
import org.apache.hadoop.io._
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

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

def custom_dot(model: GenericData.Array[Float], email: Email): Double ={
		var email_indices = email.get_vec_indices()
		var email_weights = email.get_vec_weights()
		var total =0.0
		var index = 0
		var weight = 0
		for (i <- Range(0, email_indices.length)){
			index = email_indices(i)
			weight = email_weights(i)
			total += model.get(index-1).asInstanceOf[Float] * weight
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

    //val modelFile = sc.textFile(model_filename)
    //val modelFile = sc.textFile("s3://AKIAJVLVU3XLP4GLMFEA:xZtDvTF5z0QYx5pZ8gI9KoSpcPHfKarUiNXDKGhy@largeModel/")
    //val models = sc.broadcast(parseModel(modelFile.collect()))
    
    //val distModels =sc.sequenceFile[Int, ArrayPrimitiveWritable](model_filename)
    //var models =sc.broadcast(distModels.map(mod_vec => {mod_vec._2.get()}).collect())
    //var models =sc.broadcast(distModels.map(mod_vec => {mod_vec._2.get().asInstanceOf[Array[Double]]}).collect())



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
    		subsamp_weights(gen.nextInt(len)) += 1
    		//subsamp_vec(gen.nextInt(len)).weight += 1
    	}
    	
    	for (i <- Range(0, subsamp_weights.length)){
    		subsamp_vec(i).weight = subsamp_weights(i)
    	}

    	(subsamp_id, funcs.compute_estimate(subsamp_vec.toList, 472))

    }).groupByKey().map(bootstrap_estimates =>{
    	val funcs = new run_outer_data()
    	funcs.reduce_bootstraps(bootstrap_estimates._2.toList)
    }).collect()
    
    var res = average(subsamps_out)
    System.err.println("res is:" + res)
    return res
}

