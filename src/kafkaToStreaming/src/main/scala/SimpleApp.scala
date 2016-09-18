import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON

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
    val sparkConf = new SparkConf()
                      .setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    /*
    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()
    */

    // count total events volume after each DStream
    val total = new ArrayBuffer[String]()

    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split("\n"))
    words.foreachRDD {
      rdd =>
        total ++= rdd.collect()
        println("-------------------------<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        println("Total events volume: " + total.size)
        println("Current events volume: " + rdd.count())

        rdd.collect().foreach(
          event => {
            val jsonObj = JSON.parseFull(event)
            val mapObj = jsonObj match {
              case Some(m: Map[String, String]) => m
            }
            println("------------+++++++++++++: " + mapObj("dm_id"))
            println("------------+++++++++++++: " + mapObj("dm_vid"))
            println("------------+++++++++++++: " + mapObj("dm_device"))
            println("------------+++++++++++++: " + mapObj("dm_curr_url"))
            if (mapObj("dm_action") != "view") {
              println("------------+++++++++++++: " + mapObj("dm_element"))
            }
            println("------------+++++++++++++: " + mapObj("dm_action"))
          }
        )
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

/*
build uber application jar: sbt assembly
bin/spark-submit --master local[2] --class SimpleApp /tmp/kafka_to_spark_streaming-assembly-1.0.jar localhost:9092 api

mysql.server start --log_bin=1 --binlog_format=row --server_id=2

bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' --port=5559 --producer=stdout

bin/maxwell --user='maxwell' --password='XXXXXX' --host='127.0.0.1' --port=5559 --producer=kafka --kafka.bootstrap.servers=localhost:9092  --kafka_topic=aaa # default topic: maxwell
 */