package com.bigdatasolutions.spark.sparksql

import org.apache.spark.sql.SparkSession
import com.spark2new.examples.spark2.SparkCatalogExample._
object SchemaDetection {

  def main(args: Array[String]) {
    
     val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark schema example")
      .getOrCreate()

    val df = sparkSession.read.option("header","true").option("inferSchema", "true").csv("src/main/resources/MixData.csv")

      
    println("Total number of rows " + df.count())
    df.count()
    println("Show all column data")
    df.show()
    println("Show all column schema")
    df.printSchema()
    sparkSession.stop()
  }
}
