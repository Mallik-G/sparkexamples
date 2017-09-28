package com.bigdatasolutions.spark.sparksql.database

import org.apache.spark.SparkContext

/**
 * Created by ganesh on 30/8/15.
 */
object MySqlRead {
  def main(args: Array[String]) {
    val sc : SparkContext = new SparkContext(args(0), "spark_jdbc")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val option = Map("url" -> "jdbc:mysql://localhost:3306/ecommerce", "dbtable" -> "sales","user"->"root","password"->"abc123")

    val jdbcDF = sqlContext.read.format("org.apache.spark.sql.jdbc").options(option).load()

    jdbcDF.show()
  }
}
