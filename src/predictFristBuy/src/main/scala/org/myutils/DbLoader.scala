package org.myutils

import org.apache.spark.sql.{DataFrame, SQLContext}

class DbLoader(_url: String, _username: String, _password: String, _driver: String)
{
  val url = _url
  val username = _username
  val password = _password
  val driver = _driver

  def loadWithPartition(sqlContext: SQLContext, table: String, partitionColumn: String, lowerBound: String, upperBound: String, numOfPartition: String): DataFrame = {
    val df = sqlContext.read.format("jdbc").
      option("url", url).
      option("driver", driver).
      option("dbtable", table).
      option("user", username).
      option("password", password).
      option("partitionColumn", partitionColumn).
      option("lowerBound", lowerBound).
      option("upperBound", upperBound).
      option("numPartitions", numOfPartition).
      load()
    df
  }

  def loadWithoutPartition(sqlContext: SQLContext, table: String): DataFrame = {
    val df = sqlContext.read.format("jdbc").
      option("url", url).
      option("driver", driver).
      option("dbtable", table).
      option("user", username).
      option("password", password).
      load()
    df
  }
}
