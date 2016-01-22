/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://bigdata-master:3306/sample").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "aminno_log_affiliate_clickthrough").option("user", "tester").option("password", "xxxxxxxxxxxxxxxxxxxxxxxxxxxx").load()
    df.registerTempTable("log_clicks")
    val count = sqlContext.sql("select * from log_clicks limit 999").count()

    println("count is: %s".format(count))
  }
}
