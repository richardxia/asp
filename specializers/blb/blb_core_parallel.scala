
import java.util.ArrayList;
import spark._
import SparkContext._
import javro.scala_arr

var master = "local" //?????? how to get master
var sc = new SparkContext(master, "Blb")


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

def run(data: scala_arr[Double], num_subsamples:Int, num_bootstraps:Int, subsample_len_exp:Double):Double={
	val broadcastData = sc.broadcast(data.stored)	
	var arr = data.stored
	var run_func = (x:Double)=>{
		var subsamp = subsample(broadcastData.value, subsample_len_exp)
		var bootstrap_estimates = new Array[Double](num_bootstraps)
		for (j <- Range(0, num_bootstraps)){
			var btstrap = bootstrap(subsamp)	
			bootstrap_estimates(j) = compute_estimate(btstrap)
		}
		reduce_bootstraps(bootstrap_estimates)
	}

	var subsample_estimates = sc.parallelize(new Array[Double](num_subsamples)).map(run_func)
	var ar = subsample_estimates.collect()
	return average(ar)
}

