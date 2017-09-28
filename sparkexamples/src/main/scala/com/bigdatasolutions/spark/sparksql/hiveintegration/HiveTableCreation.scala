package com.bigdatasolutions.spark.sparksql.hiveintegration

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by madhu on 7/9/15.
 */
object HiveTableCreation {

  def main(args: Array[String]) {
    val sc : SparkContext = new SparkContext(args(0), "hive metastore")
    val hiveContext = new HiveContext(sc)

    hiveContext.sql("show tables").show()

    val sales = hiveContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))
    sales.write.saveAsTable("sales")


    hiveContext.sql("show tables").show()


  }

}
