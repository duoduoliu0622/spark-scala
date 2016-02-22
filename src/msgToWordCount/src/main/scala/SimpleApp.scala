/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext}

object SimpleApp {
  val url = "jdbc:mysql://bigdata-master:3306/ashley"
  val driver = "com.mysql.jdbc.Driver"
  val user = "tester"
  val pwd = "Password@1"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("jdbc").
      option("url", url).
      option("driver", driver).
      option("dbtable", "chat_message").
      option("user", user).
      option("password", pwd).
      load()
    df.registerTempTable("chat")

    val msgDF = sqlContext.sql("select msg from chat")

    val cleaner = (msg: String) => {
      msg.toLowerCase.split(" ").map((w: String) => w.replaceAll("[^a-zA-Z0-9]", ""))
    }
    val wordDF = msgDF.explode("msg", "word")((r: String) => cleaner(r))

    wordDF.registerTempTable("words")
    val wordCount = sqlContext.sql("select word, count(1) as cnt from words group by word order by cnt desc")
    println(wordCount.count())

    save(wordCount, "chat_message_word_count")
  }

  def save(dataFrame: DataFrame, table: String): Unit = {
    val props = new java.util.Properties()
    props.setProperty("user", user)
    props.setProperty("password", pwd)
    org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.saveTable(dataFrame, url, table, props)
  }
}
