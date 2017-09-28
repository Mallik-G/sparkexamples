package com.bigdatasolutions.spark.sparksql.hiveintegration

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by madhu on 7/9/15.
 */
object HiveUDAF {
  def main(args: Array[String]) {

    val sc : SparkContext = new SparkContext(args(0), "hive udaf")
    val hiveContext = new HiveContext(sc)
    val sales = hiveContext.read.format("org.apache.spark.sql.json").option("header", "true").load(args(1))
    sales.registerTempTable("sales")
    hiveContext.sql("create temporary function custom_sum  as 'com.madhukaraphatak.sparktraining.sparksql.hiveintegration.SumUDAF'")
    hiveContext.sql("select custom_sum(amountPaid) from sales").show()
  }

}
