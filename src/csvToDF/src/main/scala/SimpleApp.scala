/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * [Note:]
 * 1. download two jars: spark-csv_2.10-1.3.0.jar, commons-csv-1.2.jar    -- no use now
 * 2. copy cars.csv to all of nodes
 */
object SimpleApp {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: SimpleApp not_live.csv  export-fix.csv
        """.stripMargin)
      System.exit(1)
    }

    val Array(notLiveCsv, exportCsv) = args
    val folder = "file:///dataDisk/s6exports/"

    val conf = new SparkConf()
        .setAppName("Simple Application")
        .set("spark.sql.crossJoin.enabled", "true")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    spark.conf.set("spark.sql.crossJoin.enabled", true)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val s6 = sc.textFile(folder + exportCsv, 8)
    val s6DF = s6.toDF("total")
    s6DF.registerTempTable("s6DF")

    val notlive = sc.textFile(folder + notLiveCsv, 8)
    val names = notlive.map(_.split("\t")).map(x => x(1))
    val namesDF = names.toDF("name")
    namesDF.registerTempTable("namesDF")
    println("---------" + namesDF.count())

    val namesOK = sqlContext.sql("select name from namesDF where length(name) > 2")
    namesOK.registerTempTable("namesOK")
    println("---------" + namesOK.count())

//    val matchDF = s6DF.join(nameDF, s6DF("total").contains(nameDF("name")))
    val matchDF = sqlContext.sql("select s.* from s6DF s join namesOK n on s.total like concat('%', n.name, '%')")
    matchDF.show(1000)

    /*
    matchDF
      .coalesce(1)
      .saveAsTextFile(folder + "matches.csv")
      */
  }
}

/*
val df = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("delimiter", "\t").option("nullValue", "null").option("treatEmptyValuesAsNulls", "true").load("file:///tmp/not_live.csv")
 */


/*
scp -i "/Users/kelinliu/.ssh/imagescraper.pem" target/scala-2.11/csv-to-df_2.11-1.0.jar ubuntu@*******:/tmp/
bin/spark-submit --master local[24] --class SimpleApp /tmp/csv-to-df_2.11-1.0.jar
/home/ubuntu/spark/bin/spark-shell --master spark://bigdata-master:7077 --packages  com.databricks:spark-csv_2.10:1.5.0, xxx:xxx:xxx
 */
