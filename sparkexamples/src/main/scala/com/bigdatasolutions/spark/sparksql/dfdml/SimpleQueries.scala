package com.bigdatasolutions.spark.sparksql.dfdml

import org.apache.spark.SparkContext

/**
 * Created by shashank on 26/8/15.
 */
object SimpleQueries {

  def main(args: Array[String]) {

    //Register the table
    val sc : SparkContext = new SparkContext(args(0), "simplequeries")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))
    salesDf.registerTempTable("sales")


    //Projection
    val itemIds = salesDf.select("itemId")
    println("Projecting itemId column from sales")
    itemIds.show()

    //Aggregation
    val totalSaleCount = salesDf.agg(("customerId","count"))
    //val totalSaleCount = sqlContext.sql("SELECT count(*) FROM sales")
    println("Counting total number of sales")
    totalSaleCount.show()

    //Group by
    val customerWiseCount = salesDf.groupBy("customerId").agg(("transactionId","count"))
    println("Customer wise sales count")
    customerWiseCount.show()

  }

}
