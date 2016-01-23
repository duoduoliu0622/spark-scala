/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val data = Array(1,2,3)
    val distData = sc.parallelize(data)
    val vectorData = distData.map(x => Vectors.dense(x))
    
    val summary = Statistics.colStats(vectorData)

    println("mean is: %s".format(summary.mean))
    println("max is: %s".format(summary.max))
    println("min is: %s".format(summary.min))
  }
}
