package com.bigdatasolutions.spark.sparksql.database

import org.apache.spark.SparkContext

/**
 * Write into parquet file
 */
object ParquetWrite {

  def main(args: Array[String]) {

    val sc : SparkContext = new SparkContext(args(0), "parquet write")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))

    salesDf.write.format("parquet").save(args(2))


  }

}
