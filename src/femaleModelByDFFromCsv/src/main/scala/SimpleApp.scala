/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext}

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}

case class PnumFeature(pnum: Int, features: Vector)

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/tmp/female_model.csv")

    df.registerTempTable("female_model")
    val goodDF = sqlContext.sql("select " +
      "pnum," +
      "days_to_last_login," +
      "total_chat_days," +
      "total_chat," +
      "max_chat_per_receiver," +
      "total_msg_days," +
      "total_msg," +
      "max_msg_per_receiver," +
      "total_login_days," +
      "total_login," +
      "total_profile_view_days," +
      "total_profile_view," +
      "max_view_per_viewee," +
      "max_distinct_contacts_hourly," +
      "day_to_max_contacts," +
      "case when fraud_status = 'not fraud' then 1 when fraud_status = 'SUSPICIOUS' then 2 when fraud_status = 'CONFIRMED' then 3 when fraud_status = 'NEW' then 4 else 5 end as is_fraud," +
      "email_action," +
      "inactive_days," +
      "mean_chat," +
      "mean_msg," +
      "mean_login," +
      "mean_profile_view" +
      " from female_model")
    df.printSchema()

    val parsedData = goodDF.map {
      case Row(
      pnum: Int,
      days_to_last_login: Int,
      total_chat_days: Int,
      total_chat: Int,
      max_chat_per_receiver: Double,
      total_msg_days: Int,
      total_msg: Int,
      max_msg_per_receiver: Double,
      total_login_days: Int,
      total_login: Int,
      total_profile_view_days: Int,
      total_profile_view: Int,
      max_view_per_viewee: Double,
      max_distinct_contacts_hourly: Int,
      day_to_max_contacts: Int,
      is_fraud: Int,
      email_action: Int,
      inactive_days: Int,
      mean_chat: Double,
      mean_msg: Double,
      mean_login: Double,
      mean_profile_view: Double
      )
      => (PnumFeature
      (pnum, Vectors.dense(
        days_to_last_login,
        total_chat_days,
        total_chat,
        max_chat_per_receiver,
        total_msg_days,
        total_msg,
        max_msg_per_receiver,
        total_login_days,
        total_login,
        total_profile_view_days,
        total_profile_view,
        max_view_per_viewee,
        max_distinct_contacts_hourly,
        day_to_max_contacts,
        is_fraud,
        email_action,
        inactive_days,
        mean_chat,
        mean_msg,
        mean_login,
        mean_profile_view
      ))
        )
    }.cache()

    val parsedDF = parsedData.toDF("pnum", "features")

    val kmeans = new KMeans()
      .setK(2)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
    val model = kmeans.fit(parsedDF)

    println("Final Centers: ")
    model.clusterCenters.foreach(println)
  }
}
