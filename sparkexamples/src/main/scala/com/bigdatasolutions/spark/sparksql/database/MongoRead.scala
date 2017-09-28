package com.bigdatasolutions.spark.sparksql.database

import org.apache.spark.SparkContext

/**
 * Created by ganesh on 31/8/15.
 */
object MongoRead {
  def main (args: Array[String]) {
    val sc: SparkContext = new SparkContext(args(0), "spark_mongo")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val options = Map("host" -> "localhost:27017", "database" -> "ecommerce", "collection" -> "sales")

    val salesDF = sqlContext.read.format("com.stratio.provider.mongodb").options(options).load

    salesDF.show

  }
}
