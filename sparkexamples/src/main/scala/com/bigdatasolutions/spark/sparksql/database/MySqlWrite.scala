package com.bigdatasolutions.spark.sparksql.database

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

import scala.collection.immutable.Map
import scala.collection.JavaConverters._



/**
 * Created by ganesh on 31/8/15.
 */
object MySqlWrite {
  def main(args: Array[String]) {
    val sc : SparkContext = new SparkContext(args(0), "spark_jdbc")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))

   val option = Map("url"->"jdbc:mysql://localhost:3306/ecommerce","dbtable"->"sales","user"->"root","password"->"abc123")

    val properties:Properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","abc123")

    salesDf.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/ecommerce","sales",properties)
  }
}
