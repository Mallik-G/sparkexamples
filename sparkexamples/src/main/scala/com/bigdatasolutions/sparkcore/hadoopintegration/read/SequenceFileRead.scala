package com.bigdatasolutions.sparkcore.hadoopintegration.read

import com.madhukaraphatak.spark.hadoopintegration.SalesRecordWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext

/**
 * Created by veera
 */
object SequenceFileRead {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"hadoopintegration")
    val dataRDD = sc.sequenceFile(args(1),classOf[NullWritable],classOf[SalesRecordWritable]).map(_._2)
    println(dataRDD.collect().toList)

  }

}
