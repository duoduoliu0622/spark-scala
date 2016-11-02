/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
 * [Note:]
 * 1. download two jars: spark-csv_2.10-1.3.0.jar, commons-csv-1.2.jar    -- no use now
 * 2. copy cars.csv to all of nodes
 */
object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val s7export = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("delimiter", ";")
      .option("nullValue", "null")
      .option("treatEmptyValuesAsNulls", "true")
      .load("file:///tmp/s7export-fix.csv")
    s7export.registerTempTable("s7")

    val notlive = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("delimiter", "\t")
      .option("nullValue", "null")
      .option("treatEmptyValuesAsNulls", "true")
      .load("file:///tmp/not_live.csv")
    notlive.registerTempTable("notlive")

    val matches = s7export.join(notlive, notlive("<Name>").equalTo(s7export("<ID>")))
    matches.show()
    println(matches.count())


    /*
   mac
      .coalesce(1)   // merge all partitions as one in case result is distributely stored on all nodes
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("file:///tmp/new.csv")
      */
  }
}

/*
val df = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("delimiter", "\t").option("nullValue", "null").option("treatEmptyValuesAsNulls", "true").load("file:///tmp/not_live.csv")
 */


/*
/home/ubuntu/spark/bin/spark-shell --master spark://bigdata-master:7077 --packages  com.databricks:spark-csv_2.10:1.5.0, xxx:xxx:xxx
 */
