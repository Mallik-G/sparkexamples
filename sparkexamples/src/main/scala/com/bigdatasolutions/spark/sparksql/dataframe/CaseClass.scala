package com.bigdatasolutions.spark.sparksql.dataframe

import com.madhukaraphatak.sparktraining.sparksql.utils.Sales
import org.apache.spark._

/**
 * Creating Dataframe from case classes
 */
object CaseClass {


  def main(args: Array[String]) {
    val sc : SparkContext = new SparkContext(args(0), "caseclass")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val sales  = sc.textFile(args(1)).filter(line => !line.startsWith("transactionId"))
      .map(_.split(","))
      .map(p=> Sales(p(0).trim.toInt,p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))

    val salesDF = sqlContext.createDataFrame(sales)
    salesDF.show()


  }
}
