package com.bigdatasolutions.spark.sparksql.datasource

import org.apache.spark.SparkContext

/**
 * Created by hadoop on 15/2/15.
 */
object JsonFile {

  def main(args: Array[String]) {
    val sc : SparkContext = new SparkContext(args(0), "jsonfile")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val sales = sqlContext.read.format("org.apache.spark.sql.json").load(args(1))
    sales.printSchema()

    sales.show()

  }

}
