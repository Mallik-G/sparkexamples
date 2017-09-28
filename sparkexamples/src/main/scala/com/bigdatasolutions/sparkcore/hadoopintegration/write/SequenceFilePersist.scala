package com.bigdatasolutions.sparkcore.hadoopintegration.write

import com.bigdatasolutions.sparkcore.apiexamples.serilization.SalesRecordParser
import com.madhukaraphatak.spark.hadoopintegration.SalesRecordWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by veera
 */
object SequenceFilePersist {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "hadoopintegration")
    val dataRDD = sc.textFile(args(1))
    val outputPath = args(2)
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    val salesRecordWritableRDD = salesRecordRDD.map(salesRecord => {
      (NullWritable.get(), new SalesRecordWritable(salesRecord.transactionId, salesRecord.customerId,
        salesRecord.itemId, salesRecord.itemValue))
    })

    salesRecordWritableRDD.saveAsSequenceFile(outputPath)
  }


}
