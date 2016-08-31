import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.myutils.Credentials

object SimpleApp {
  val url = "jdbc:mysql://bigdata-master:3306/fraud"
  val driver = "com.mysql.jdbc.Driver"

  var user: String = ""
  var pwd: String = ""

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val handler = new Credentials("/opt/bigdata/credentials.ini").handler()
    user = handler.get("local-db", "username")
    pwd = handler.get("local-db", "password")

    val loadTable = {
      table: String =>
        sqlContext.read.format("jdbc").
          option("url", url).
          option("driver", driver).
          option("dbtable", table).
          option("user", user).
          option("password", pwd).
          load()
    }

    val df = loadTable("one_day")
    val variables = List("country", "gender")

    variables.foreach(v => {
      val data = df.map{
          case Row(pnum: Int, country: Int, gender: Int, seeking: Int, age: Int, ethnic: Int, vids: Int, vid_linked_fraud:Int, ips: Int, ip_linked_fraud: Int, emails:Int, email_linked_fraud:Int, is_fraud_domain: Int, is_same: Int, is_fraud: Int) =>
            Vectors.dense(country.toDouble, is_fraud.toDouble)
      }

      val corr = Statistics.corr(data)
      println(corr.toString())
    })
  }
}
