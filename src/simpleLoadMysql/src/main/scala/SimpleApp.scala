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

    val df = sqlContext.read.format("jdbc").
option("url", "jdbc:mysql://bigdata-master:3306/sample").
option("driver", "com.mysql.jdbc.Driver").
option("dbtable", "achat").
option("user", System.getenv("MYSQL_USERNAME")).
option("password", System.getenv("MYSQL_PASSWORD")).
option("partitionColumn", "hp").
option("lowerBound", "0").
option("upperBound", "44000000").
option("numPartitions", "5").
load()
    df.registerTempTable("achat")
    val someRows = sqlContext.sql("select hp, count(distinct up) as cnt from achat group by hp order by cnt desc").head()

    println("--------see here!------->" + someRows.mkString(" "))
  }
}
