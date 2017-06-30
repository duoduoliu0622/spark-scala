/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SQLContext}
import org.myutils.{Credentials, DbSaver}

/**
 * [Note:]
 * 1. download two jars: spark-csv_2.10-1.3.0.jar, commons-csv-1.2.jar    -- no use now
 * 2. copy cars.csv to all of nodes
 */
object SimpleApp {
  case class LineObj(ID: String, Name: String, Object_Type_Name: String, Legacy_ID: String, Colour: String, Colour_Family: String, Colour_ID: String, Supplier_Colour_Name: String, Supplier_Colour_Name2: String, Temporary_Colour: String)


  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
                            |Usage: SimpleApp input.xml
        """.stripMargin)
      System.exit(1)
    }

    val Array(inputXml) = args
    val folder = "file:///dataDisk/s6exports/"

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val user = "root"
    val pwd = "P)$CC`SEJB{4n8Yk"
    val url = "jdbc:mysql://127.0.0.1:3306/kelin"
    val driver = "com.mysql.jdbc.Driver"

    /*
    val s6 = sc.textFile(folder + inputXml, 16)
    val s6updated = s6.map{
      line =>
        if (line.contains("UserTypeID=\"Item\"")) line.replaceFirst("<Product", "<Product_Item")
        else if (line.contains("UserTypeID=\"SKU\"")) line.replaceFirst("<Product", "<Product_SKU")
        else if (line.contains("UserTypeID=\"ProductGroup\"")) line.replaceFirst("<Product", "<Product_ProductGroup")
        else line
    }

    val newXml = "_s6updated.xml/part-00000"
    s6updated.repartition(1).saveAsTextFile(folder + newXml)
*/

    val newXml = "_s6updated.xml/part-00000"
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "Product_ProductGroup")
      .load(folder + newXml)

    val dbSaver = new DbSaver(url, user, pwd, driver)
    dbSaver.createAndSave(df, "s6xml")






/*
s7DF
    .coalesce(1)
  .write
  .format("com.databricks.spark.csv")
  .option("header", "false")
  .save(folder + resultFolder)

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

../spark/bin/spark-submit --master local[8] --driver-memory 12g --packages com.databricks:spark-xml_2.10:0.4.1,mysql:mysql-connector-java:5.1.40 --class SimpleApp csv-to-df_2.11-1.0.jar s6export.xml
*/
