package com.bigdatasolutions.sparkcore.anatomy.laziness

import org.apache.spark.SparkContext

/**
 * Created by veera on 15/05/16.
 */
object RunJob {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "run job example")
    val salesData = sc.textFile(args(1))

    println("built in collect " +salesData.collect().toList)
    //implement collect using runJob API

    val results = sc.runJob(salesData, (iter: Iterator[String]) => iter.toArray)
    val collectedResult = Array.concat(results: _*).toList
    println("result of custom collect " +collectedResult)


  }

}
