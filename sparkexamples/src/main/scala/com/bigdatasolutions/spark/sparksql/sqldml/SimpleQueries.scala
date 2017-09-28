package com.bigdatasolutions.spark.sparksql.sqldml

import com.madhukaraphatak.sparktraining.sparksql.utils.Sales
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
 * Running basic queries on schemaRDD
 */
object SimpleQueries {
  def main(args: Array[String]) {

    //Register the table
    val sc : SparkContext = new SparkContext(args(0), "simplequeries")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))
    salesDf.registerTempTable("sales")


    //Projesction
    val itemids = sqlContext.sql("SELECT itemId FROM sales")
    println("Projecting itemId column from sales")
    itemids.show()

    //Aggregation
    val totalSaleCount = sqlContext.sql("SELECT count(*) FROM sales")
    println("Counting total number of sales")
    totalSaleCount.show()

    //Group by
    val customerWiseCount = sqlContext.sql("SELECT customerId,count(*) FROM sales group by customerId")
    println("Customer wise sales count")
    customerWiseCount.show()

  }
}
