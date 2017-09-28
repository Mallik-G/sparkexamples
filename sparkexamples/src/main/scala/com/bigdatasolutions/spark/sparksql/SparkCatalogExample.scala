package com.bigdatasolutions.spark.sparksql

import org.apache.spark.sql.SparkSession


/**
  * Catalogue Example
  */
object SparkCatalogExample {

  def main(args: Array[String]) {


//In spark 1.6 ,spark context was entry point for Spark,it differ for different different purpose for sql opearation ,we needed sqlContext ,for streaming purpose ,we needed streamingContext. 

//But from Spark 2.0 there is a new entry point that is sparkSession.It is combination of SQLContext, HiveContext.

    val sparkSession = SparkSession.builder.
           master("local")
           .appName("example")
           .getOrCreate()


//It is same as sqlContext work.
println("Now,we are going to read mariadata_200.csv .It is same as sqlContext work")
    val df = sparkSession.read.option("header","true").csv("src/main/resources/mariadata_200.csv")


println("Now,we are going to read applestock.csv .It is same as sqlContext work"
)
    val df2 = sparkSession.read.option("header","true").csv("src/main/resources/MixData.csv")




//In spark 1.6 ,we used registerTempTable to register a dataframe but now this API is deprecated.Now ,createTempView is used for that.

println("In spark 1.6 ,we used registerTempTable to register a dataframe but now this API is deprecated.Now ,createTempView is used for that")
    df.createTempView("mariadata_200")
   df2.createTempView("apple")

println("It would print mariadata_200 csv data with column .")
df.show()
println("It would print applestock csv data with column .")
df2.show()


println("Now we are going to use catalog API")
    val catalog = sparkSession.catalog

println("It would print the databases list.If you are not using Hive ,then it show 'default' database")

    catalog.listDatabases().select("name").show()

  println("It would print the table list")

    catalog.listTables().select("name").show()

println("Checking for mariadata_200 table is cached or not.Till now,we didn't cache any table so it would show false.")
    
    println(catalog.isCached("mariadata_200"))

println("Now,we are going to cache mariadata_200 table")
    df.cache()

println("Now,we are going to check ,that mariadata_200 table is cached or not")
    println(catalog.isCached("mariadata_200"))

    // drop the table
println("Now,we are going drop the table mariadata_200")
    catalog.dropTempView("mariadata_200")
    catalog.listTables().select("name").show()

    sparkSession.stop
  }

}
