package com.bigdatasolutions.sparkcore.apiexamples.errorhandling

import com.bigdatasolutions.sparkcore.apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by veera on
 */
object HandleMalformedRecords {

  def main(args: Array[String]) {


    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))

    val validatedRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      if(parseResult.isLeft){
        (false,row)
      }
      else (true,row)
    })

    val malformedRecords = validatedRDD.filter(pair => pair._1 == false).map(_._2)
    val normalRecords = validatedRDD.map(_._2).subtract(malformedRecords)

    val salesRecordRDD = normalRecords.map(row => SalesRecordParser.parse(row).right.get)

    println(malformedRecords.collect().toList)
    println(normalRecords.collect().toList)


  }

}
