package com.bigdatasolutions.sparkcore.hadoopintegration.read

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext

/**
 * Created by veera
 */
object TextFileRead {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"hadoopintegration")

    //actual textFile api converts to the following code
    val dataRDD = sc.hadoopFile(args(1), classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      sc.defaultMinPartitions).map(pair => pair._2.toString)

    println(dataRDD.collect().toList)

  }


}
