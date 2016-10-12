import java.math.BigInteger

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON

import org.myutils.DbSaver

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */
object SimpleApp{

  case class EventObj(username: String, text: String)

  val url = "jdbc:mysql://localhost:3306/main"
  val driver = "com.mysql.jdbc.Driver"

  var username: String = "root"
  var password: String = "123456"

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val conf = new SparkConf().setAppName("KafkaTweetStreaming")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val dbSaver = new DbSaver(url, username, password, driver)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // count total events volume after each DStream
    var total: Long = 0

    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split("\n"))
    words.foreachRDD {
      rdd =>
        total = total + rdd.count()
        println("-------------------------<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        println("Total events volume: " + total)
        println("Current events volume: " + rdd.count())

        val df = rdd.map {
          event: String => {
            val jsonObj = JSON.parseFull(event)
            var mapObj = jsonObj match {
              case Some(m: Map[String, String]) => m
            }
            new EventObj(mapObj("username"), mapObj("text"))
          }
        }.toDF()

        df.show()
        dbSaver.append(df, "tweets")
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

/*
build uber application jar: sbt assembly
bin/spark-submit --master local[2] --class SimpleApp /tmp/kafka_to_spark_streaming-assembly-1.0.jar localhost:9092 api
 */