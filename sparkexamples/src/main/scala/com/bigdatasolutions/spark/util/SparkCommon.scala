package com.bigdatasolutions.spark.util

/*import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}*/

import com.typesafe.config._
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext._

object SparkCommon {

  lazy val conf = {
    new SparkConf(false)
      .setMaster("local[*]")
      .setAppName("Spark Tutorial")
  }

  lazy val sparkContext = new SparkContext(conf)
 /* lazy val sparkSQLContext = SQLContext.getOrCreate(sparkContext)
  lazy val streamingContext = StreamingContext.getActive()
    .getOrElse(new StreamingContext(sparkContext, Seconds(2)))*/

}
