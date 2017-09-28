package com.bigdatasolutions.spark.sparksql.database

import org.apache.spark.SparkContext

/**
 * Created by ganesh on 31/8/15.
 */
object ParquetRead {
  def main (args: Array[String]) {
    val sc: SparkContext = new SparkContext(args(0), "parquet read")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val salesDF = sqlContext.read.format("parquet").load(args(1))

    salesDF.show

  }
}
