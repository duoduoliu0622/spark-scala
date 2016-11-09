/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row,SQLContext}
import org.apache.spark.storage.StorageLevel

/**
 * [Note:]
 * 1. download two jars: spark-csv_2.10-1.3.0.jar, commons-csv-1.2.jar    -- no use now
 * 2. copy cars.csv to all of nodes
 */
object SimpleApp {
  case class LineObj(ID: String, Name: String, Object_Type_Name: String, Legacy_ID: String, Colour: String, Colour_Family: String, Colour_ID: String, Supplier_Colour_Name: String, Supplier_Colour_Name2: String, Temporary_Colour: String)


  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: SimpleApp input.csv result_folder
        """.stripMargin)
      System.exit(1)
    }

    val Array(inputCsv, resultFolder) = args
    val folder = "file:///dataDisk/s6exports/"

    val conf = new SparkConf()
        .setAppName("Simple Application")
        .set("spark.sql.crossJoin.enabled", "true")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    spark.conf.set("spark.sql.crossJoin.enabled", true)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val s7 = sc.textFile(folder + inputCsv, 8)
    val s7Index = s7.map{
        line =>
          val _line = line + ";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;_lkl"
          val fields = _line.split(";")
          new LineObj(fields(0), fields(1), fields(3), fields(4), fields(49), fields(50), fields(51), fields(174), fields(175), fields(181))
    }
    val s7DF = s7Index.toDF()

    /*
    val notlive = sc.textFile(folder + notLiveCsv, 8)
    val names = notlive.map(_.split("\t")).map(x => x(1))
    val namesDF = names.toDF("name")
    namesDF.registerTempTable("namesDF")
    println("---------" + namesDF.count())

    val namesOK = sqlContext.sql("select name from namesDF where length(name) > 2")
    namesOK.registerTempTable("namesOK")
    println("---------" + namesOK.count())

//    val matchDF = s6DF.join(nameDF, s6DF("total").contains(nameDF("name")))
    val matchDF = sqlContext.sql("select s.total from s6DF s join namesOK n on n.name = s.id")
    matchDF.show(10)
    println(matchDF.count())
    */

    s7DF
        .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .save(folder + resultFolder)

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
scp -i "/Users/kelinliu/.ssh/imagescraper.pem" target/scala-2.11/csv-to-df_2.11-1.0.jar ubuntu@monster:/tmp/
bin/spark-submit --master local[8] --driver-memory 4g --executor-memory 2g --class SimpleApp /tmp/csv-to-df_2.11-1.0.jar not_live_without_head.csv s6export-fix.csv
601-82925-000 in not-live,  not in match, in s6export.csv,

/home/ubuntu/spark/bin/spark-shell --master spark://bigdata-master:7077 --packages  com.databricks:spark-csv_2.10:1.5.0, xxx:xxx:xxx
 */
