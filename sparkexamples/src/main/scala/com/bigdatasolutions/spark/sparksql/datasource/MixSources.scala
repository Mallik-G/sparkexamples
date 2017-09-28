package com.bigdatasolutions.spark.sparksql.datasource

import org.apache.spark.SparkContext

import scala.util.Try

/**
 * Mixed sources
 */
object MixSources {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "Csv loading example")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val path = args(1)
    val source = args(2)
    val header = Try(args(3)).getOrElse("true")
    val df = sqlContext.read.format(source).options(Map("path" -> path, "header" -> header)).load()
    df.printSchema()
    df.show()
  }
}
