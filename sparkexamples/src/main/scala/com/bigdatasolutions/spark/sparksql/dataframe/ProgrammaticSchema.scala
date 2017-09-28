package com.bigdatasolutions.spark.sparksql.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}


object ProgrammaticSchema {

  def main(args: Array[String]) {

    val sc : SparkContext = new SparkContext(args(0), "programmaticschema")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val salesRDD = sc.textFile(args(1))

    val schema =
      StructType(
        Array(StructField("transactionId", IntegerType, true),
          StructField("customerId", IntegerType, true),
          StructField("itemId", IntegerType, true),
          StructField("amountPaid", DoubleType, true))
      )

    val rowRDD = salesRDD.filter(line => !line.startsWith("transactionId"))
      .map(_.split(",")).map(p => Row(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))

    val salesDF = sqlContext.createDataFrame(rowRDD, schema)
    salesDF.show()

  }

}
