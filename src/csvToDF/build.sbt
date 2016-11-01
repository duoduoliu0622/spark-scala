name := "csv to DF"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.11"  % "2.0.0",
  "org.apache.commons" % "commons-csv" % "1.4",
  "com.databricks" % "spark-csv_2.11" % "1.5.0"
)
