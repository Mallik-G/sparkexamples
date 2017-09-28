package com.bigdatasolutions.sparkcore.extend

import com.bigdatasolutions.sparkcore.apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext

/**
 * Created by veera on 27/2/15.
 */
object CustomOperatorExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })
    import com.madhukaraphatak.spark.extend.CustomOperators._
    salesRecordRDD.totalAmount
  }

}
