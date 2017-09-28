package com.bigdatasolutions.sparkcore.hadoopintegration.read

import com.bigdatasolutions.sparkcore.apiexamples.serilization.SalesRecord
import com.madhukaraphatak.spark.hadoopintegration.SalesRecordWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext

/**
 * Created by madhu.
 */
object ObjectFileRead {
  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"apiexamples")
    val dataRDD = sc.objectFile[SalesRecord](args(1))
    println(dataRDD.collect().toList)

  }

}
