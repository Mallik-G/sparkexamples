package com.bigdatasolutions.sparkcore.apiexamples.advanced

import com.bigdatasolutions.sparkcore.apiexamples.serilization.{SalesRecord, SalesRecordParser}
import org.apache.spark.SparkContext

/**
 * Created by veera
 */
object Fold {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })
    //give me salesRecord which has max value
    val dummySalesRecord = new SalesRecord(null,null,null,0)
    val maxSalesRecord = salesRecordRDD.fold(dummySalesRecord)((acc,salesRecord)=>{
      if(acc.itemValue < salesRecord.itemValue) salesRecord else acc
    })
    println("max sale record is "+ maxSalesRecord)
  }
}
