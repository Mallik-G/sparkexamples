package com.bigdatasolutions.spark.sparksql.sqldml

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by madhu on 25/2/15.
 */
object UDFExamples {

  def main(args: Array[String]) {

    val sc : SparkContext = new SparkContext(args(0), "caseclass")
    val sqlContext = new SQLContext(sc)

    val salesDf = sqlContext.read.format("org.apache.spark.sql.json").load(args(1))
    salesDf.registerTempTable("sales")

    val toInt = (input:Double) => input.toInt
    sqlContext.udf.register("toInt",toInt)

    val results = sqlContext.sql("select customerId,toInt(amountPaid) from sales")
    results.show()

  }

}
