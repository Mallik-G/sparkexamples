package com.bigdatasolutions.spark.sparksql.datasource

import org.apache.spark.SparkContext

/**
 * Created by shashank on 26/8/15.
 */
object CsvFile {

  def main(args: Array[String]) {

    val sc : SparkContext = new SparkContext(args(0), "csvfile")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val sales = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))
    sales.printSchema()

    sales.show()

  }

}
