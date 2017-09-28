package com.bigdatasolutions.spark.sparksql.streaming

import com.madhukaraphatak.sparktraining.sparksql.utils.Sales
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

/**
 * Created by madhu on 25/2/15.
 */
object StreamingExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"Streaming wordcount")
    val ssc = new StreamingContext(sc, Seconds(10))
    val networkStream = ssc.socketTextStream("localhost",50050)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    val salesStream = networkStream.map(row => {
      val columnValues = row.split(",")
      new Sales(columnValues(0).toInt,columnValues(1).toInt,columnValues(2).toInt,columnValues(3).toDouble)
    })

    salesStream.foreachRDD(rdd => {
      sqlContext.createDataFrame(rdd).registerTempTable("sales")
      val totalSaleCount = sqlContext.sql("SELECT count(*) FROM sales")
      println("Total sales output")
      println(totalSaleCount.map(t => "Number of sales: " + t(0)).collect().toList)
    })


    ssc.start()
    ssc.awaitTermination()

  }

}
