package com.bigdatasolutions.spark.sparksql.sparksql.DataSources

import com.bigdatasolutions.spark.util.SparkCommon
import org.apache.spark.sql.SQLContext

/**
  * Created by veera on 29/1/16.
  */
object CsvFile {

  val sc = SparkCommon.sparkContext

  val sqlContext = SparkCommon.sparkSQLContext

  def main(args: Array[String]) {

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("src/main/resources/cars.csv")
    df.show()
    df.printSchema()

    val selectedData = df.select("year", "model")
    selectedData.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
    selectedData.show()


    //.save(s"src/main/resources/${UUID.randomUUID()}")
    //println("OK")

  }

}


