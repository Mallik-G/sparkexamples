package com.bigdatasolutions.spark.sparksql.hiveintegration

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by madhu on 7/9/15.
 */
object HiveUDF {
  def main(args: Array[String]) {

    val sc : SparkContext = new SparkContext(args(0), "hive udf")
    val hiveContext = new HiveContext(sc)
    val customers = hiveContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))
    customers.registerTempTable("customers")
    hiveContext.sql("create temporary function custom_lower as 'com.madhukaraphatak.sparktraining.sparksql.hiveintegration.Lower'")
    hiveContext.sql("select custom_lower(name) from customers").show()
  }

}
