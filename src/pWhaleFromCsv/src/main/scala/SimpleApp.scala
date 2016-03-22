/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext}

object SimpleApp {
  val url = "jdbc:mysql://bigdata-master:3306/ashley"
  val driver = "com.mysql.jdbc.Driver"
  val user = System.getenv("MYSQL_USERNAME")
  val pwd = System.getenv("MYSQL_PASSWORD")

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/tmp/paid_mont.csv")

    df.registerTempTable("paid")
    val ttc = sqlContext.sql("select pnum, count(1) as times, min(p_month) as first_month, max(p_month) as last_month, sum(paid) as total from paid group by pnum").cache()

    ttc.registerTempTable("total_times")
    val result = sqlContext.sql("select * from total_times where (times> 10 or total > 350) and last_month >= '2015-07' and last_month < '2015-11'")

    result.collect().foreach(println)
    println("--------total count: " + result.count())

    sc.stop()
  }

  def save(dataFrame: DataFrame, table: String): Unit = {
    val props = new java.util.Properties()
    props.setProperty("user", user)
    props.setProperty("password", pwd)
    props.setProperty("driver", driver)

    // create and save in table
    dataFrame.write.jdbc(url, table, props)
  }
}


/*
if table already exist
---------------------------------------------------------------------------------------------
sqlContext.read.format("jdbc").
        option("url", url).
        option("driver", driver).
        option("dbtable", "cluster_in_test").
        option("user", user).
        option("password", pwd).
        load()

org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.saveTable(dataFrame, url, table, props)
 */
