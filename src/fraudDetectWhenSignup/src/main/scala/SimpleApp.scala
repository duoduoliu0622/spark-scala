import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import org.myutils.{DbSaver, Credentials}

object SimpleApp {
  val url = "jdbc:mysql://bigdata-master:3306/fraud"
  val driver = "com.mysql.jdbc.Driver"

  var user: String = ""
  var pwd: String = ""

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

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

    val data = df.map{
      case Row(pnum: Int, age: Int, ethnic: Int, vids: Int, ips: Int, emails:Int,  caption_len: Int, bodytype:Int, profile_initially_seeking:Int, is_fraud: Int) =>
        LabeledPoint(is_fraud.toDouble, Vectors.dense(age.toDouble, ethnic.toDouble, vids.toDouble, ips.toDouble, emails.toDouble, caption_len.toDouble, bodytype.toDouble, profile_initially_seeking.toDouble))
    }.toDF()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(32)
      .fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)
    predictions.select("predictedLabel", "label", "features").show(5)

    val dbSaver = new DbSaver(url, user, pwd, driver)
    dbSaver.createAndSave(predictions.select("predictedLabel", "label"), "result")

    // evaluation
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("-----------Test Error = " + (1.0 - accuracy))

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("-----------Learned classification forest model:\n" + rfModel.toDebugString)

    // start predicting new users
    val dfNew = loadTable("one_day_copy")

    val dataNew = dfNew.map{
      case Row(pnum: Int, age: Int, ethnic: Int, vids: Int, ips: Int, emails:Int,  caption_len: Int, bodytype:Int, profile_initially_seeking:Int, is_fraud: Int) =>
        LabeledPoint(pnum.toDouble, Vectors.dense(age.toDouble, ethnic.toDouble, vids.toDouble, ips.toDouble, emails.toDouble, caption_len.toDouble, bodytype.toDouble, profile_initially_seeking.toDouble))
    }.toDF()

    val pred = model.transform(dataNew)
    pred.select("predictedLabel", "label", "features").show()
  }
}
