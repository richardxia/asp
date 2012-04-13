import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;

	
def bootstrap( data: org.apache.avro.generic.GenericData.Array[Double]): org.apache.avro.generic.GenericData.Array[Double] ={
    var str = "{\"type\": \"array\", \"items\": {\"type\": \"array\", \"items\": \"double\"}}"
    var schema: Schema = (new Schema.Parser()).parse(str)
    var bootstrap_vectors = new org.apache.avro.generic.GenericData.Array[org.apache.avro.generic.GenericData.Array[Double]](1,schema)
    str = "{\"type\": \"array\", \"items\": \"double\"}"
    schema = (new Schema.Parser()).parse(str)
    var subL0 = new org.apache.avro.generic.GenericData.Array[Double](1,schema)
    bootstrap_vectors.add(subL0)
    for (i <-Range(0,data.size())) {
        var d =  scala_lib.slice(data, (i * DIM), ((i + 1) * DIM))
        bootstrap_vectors.add(d)
    }
    str = "{\"type\": \"array\", \"items\": {\"type\": \"array\", \"items\": \"double\"}}"
    schema = (new Schema.Parser()).parse(str)
    var bootstrap = new org.apache.avro.generic.GenericData.Array[org.apache.avro.generic.GenericData.Array[Double]](1,schema)
    str = "{\"type\": \"array\", \"items\": \"double\"}"
    schema = (new Schema.Parser()).parse(str)
    var subL1 = new org.apache.avro.generic.GenericData.Array[Double](1,schema)
    bootstrap.add(subL1)
    for (i <- Range(0,(data.size() / DIM))) {
        bootstrap.add(scala_lib.rand_choice(bootstrap_vectors))
    }
    schema = (new Schema.Parser()).parse(str)
    var flat = new org.apache.avro.generic.GenericData.Array[Double](1,schema)
    for (i <-Range(0,bootstrap.size())) {
        var item = bootstrap.get(i)
        for (j <-Range(0,item.size())) {
            flat.add(item.get(j))
        }
    }
    return flat
}

def subsample(data: org.apache.avro.generic.GenericData.Array[Double], subsample_len_exp:Double):
	org.apache.avro.generic.GenericData.Array[Double]= {
		var subsample_len = math.pow(data.size(), subsample_len_exp).asInstanceOf[Int]
        var str = "{\"type\": \"array\", \"items\": \"double\"}"	
        var schema: Schema = (new Schema.Parser()).parse(str)
        var sample_arr = new org.apache.avro.generic.GenericData.Array[Double](1,schema)
        for (num <- Range(0, ((data.size()/ DIM) - 1))){
        	sample_arr.add((num + 1))
        }
		var subsample_indices = scala_lib.rand_sample(sample_arr, subsample_len)
		var subsamp = new org.apache.avro.generic.GenericData.Array[Double](1, schema)
		for ( i <- Range(0, subsample_indices.size())){
			var index = subsample_indices.get(i)
			var d = scala_lib.slice(data, (index * DIM).asInstanceOf[Int], 
									((index + 1)*DIM).asInstanceOf[Int])
			for (j <- Range(0, d.size())) {
				subsamp.add(d.get(j))
			}
		}
		return subsamp
}

def run(data: org.apache.avro.generic.GenericData.Array[Double], num_subsamples: Int, num_bootstraps:Int,
	subsample_len_exp:Double): Double = {		
		var str = "{\"type\": \"array\", \"items\": \"double\"}"
		var schema: Schema = (new Schema.Parser()).parse(str)
		var subsample_estimates = new org.apache.avro.generic.GenericData.Array[Double](1, schema)
		for (i<- Range(0, num_subsamples-1)){
			var subsamp = subsample(data, subsample_len_exp)
			str = "{\"type\": \"array\", \"items\": \"double\"}"
			schema = (new Schema.Parser()).parse(str)
			var bootstrap_estimates = new org.apache.avro.generic.GenericData.Array[Double](1, schema)
			for (j <- Range(0, num_bootstraps)){
				var btstrap = bootstrap(subsamp)
				var estimate = compute_estimate(btstrap)
				bootstrap_estimates.add(estimate)
			}
			var subsample_est = reduce_bootstraps(bootstrap_estimates)												
			subsample_estimates.add(subsample_est)
		}
		return average(subsample_estimates)
}

