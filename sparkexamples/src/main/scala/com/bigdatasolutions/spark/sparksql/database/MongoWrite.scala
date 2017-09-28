package com.bigdatasolutions.spark.sparksql.database

import org.apache.spark.SparkContext
import org.apache.spark.sql._


/**
 * Created by ganesh on 30/8/15.
 */
object MongoWrite {
   def main (args: Array[String]) {
     val sc : SparkContext = new SparkContext(args(0), "spark_mongo")
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)

     val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))

     val options = Map("host" -> "localhost:27017", "database" -> "ecommerce", "collection" -> "sales")

     salesDf.write.format("com.stratio.provider.mongodb").mode(SaveMode.Append).options(options).save()

  }
}
