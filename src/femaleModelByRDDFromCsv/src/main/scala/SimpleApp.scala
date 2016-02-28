/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

case class WhichCluster(cluster_id: Int)

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
    .load("/tmp/new_female_model.csv")

    df.registerTempTable("female_model")
    val goodDF = sqlContext.sql("select " +
      "hour_from_signup," +
      "max_volume," +
      "fraud_status," +
      "weeks_from_signup," +
      "email_action" +
      " from female_model")
    df.printSchema()

    val parsedData = goodDF.map {
      case Row(
      hour_from_signup: Int,
      max_volume: Int,
      fraud_status: Int,
      weeks_from_signup: Int,
      email_action: Int
      )
      => (Vectors.dense(
        hour_from_signup,
        max_volume,
        fraud_status,
        weeks_from_signup,
        email_action
      ))
    }.cache()


    val list = List(2,3,4,5)

    list.foreach(n => {
      val numClusters = n
      val numIterations = 20
      val clusters = KMeans.train(parsedData, numClusters, numIterations)

      println("----------------------" + numClusters + "--------------------------")

      val resultRDD = clusters.predict(parsedData)
      val resultDF = resultRDD.map(r => WhichCluster(r)).toDF()

      val countDF = resultDF.groupBy("cluster_id").count()

      println(" PMML Model :\n" + clusters.toPMML)
      countDF.show

      save(countDF, "cluster_in_test" + n)
    })
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
