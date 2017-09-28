package com.bigdatasolutions.spark


import com.typesafe.config._
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {
    val appConf = ConfigFactory.load()
    /*val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(appConf.getConfig(args(2)).getString("executionMode"))

    for(c <- conf.getAll)
      println(c._2)*/

    val conf = new SparkConf().
      setAppName("Word Count").setMaster("local")

    val sc = new SparkContext(conf)
    val inputPath = args(0)
    val outputPath = args(1)

    // We need to use HDFS FileSystem API to perform validations on input and output path
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPathExists = fs.exists(new Path(inputPath))
    val outputPathExists = fs.exists(new Path(outputPath))

    if(!inputPathExists) {
      println("Invalid input path")
      return
    }

    if(outputPathExists)
      fs.delete(new Path(outputPath), true)

    val file = sc.textFile(inputPath).flatMap(line=> line.split(" ")).map(word=> (word, 1)).reduceByKey((acc, value) => acc + value)


    // changing tuple to delimited text before saving the output
    // We can also use map(rec => rec._1 + "\t" + rec._2)
    file.map(rec => rec.productIterator.mkString("\t")).
      saveAsTextFile(outputPath)

  }
}