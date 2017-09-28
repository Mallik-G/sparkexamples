package com.bigdatasolutions.sparkcore.hadoopintegration.write

import com.bigdatasolutions.sparkcore.apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext

/**
 * Created by veera
 */
object ObjectFilePersist {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "hadoopintegration")
    val dataRDD = sc.textFile(args(1))
    val outputPath = args(2)
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    salesRecordRDD.saveAsObjectFile(outputPath)

  }

}
