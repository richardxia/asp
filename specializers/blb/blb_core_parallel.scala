
/**
import java.util.ArrayList;
import spark._
import SparkContext._
import javro.scala_arr

var sc = new SparkContext(System.getenv("MASTER"), "Blb", "/root/spark", List(System.getenv("FILE_LOC")))

def run(data: scala_arr[Double], num_subsamples:Int, num_bootstraps:Int, subsample_len_exp:Double):Double={
	val broadcastData = sc.broadcast(data.stored)	
	val bnum_bootstraps = sc.broadcast(num_bootstraps)
	val bsubsample_len_exp = sc.broadcast(subsample_len_exp)

	var run_func = (x:Double)=>{

**/
		def bootstrap(data: Array[Double]): Array[Double] = {
			var bootstrap_vectors = new Array[Array[Double]](data.length/DIM)
		
			for (i <- Range(0, data.length/DIM)){
				var store_arr = new Array[Double](data.length)
				var count = 0
				for (j <- Range(i*DIM, (i+1)*DIM)){
					store_arr(count)=data(j)
					count+=1
				}			
				bootstrap_vectors(i) = store_arr
			}
			var bootstrap = new Array[Double](data.length)
			var len = bootstrap_vectors.length
			var gen = new java.util.Random()
			
			for (i <- Range(0, data.length/DIM)){
				var store_arr = bootstrap_vectors(gen.nextInt(len))
				for (j <- Range(0, DIM)){
					bootstrap(i*DIM + j) = store_arr(j)
				}
			}
			return bootstrap
		}
		
		
		def subsample(data: ArrayList[Object], subsample_len_exp:Double): Array[Double] = {
			var subsample_len = math.pow(data.size(), subsample_len_exp).asInstanceOf[Int]
			var subsample_indicies = new Array[Int](subsample_len)
			var gen = new java.util.Random()
			var len = data.size()
			for (i <- Range(0, subsample_len)){
				subsample_indicies(i) = gen.nextInt(len/ DIM)
			}
		
			var subsample = new Array[Double](subsample_len*DIM)
			var count = 0
			for (s <- Range(0, subsample_indicies.length)){
				var index = subsample_indicies(s)
				for (j <- Range((index*DIM).asInstanceOf[Int], ((index+1)*DIM).asInstanceOf[Int])){
					subsample(count) = data.get(j).asInstanceOf[Double]
					count += 1
				}
			}
			return subsample
		}
		
		var subsamp = subsample(broadcastData.value, bsubsample_len_exp.value)
		var bootstrap_estimates = new Array[Double](bnum_bootstraps.value)
		for (j <- Range(0, bnum_bootstraps.value)){

			var btstrap = bootstrap(subsamp)	
			bootstrap_estimates(j) = compute_estimate(btstrap)
		}
		reduce_bootstraps(bootstrap_estimates)
	}


	//return average( sc.parallelize(new Array[Double](num_subsamples)).map(run_func).collect() ) }
