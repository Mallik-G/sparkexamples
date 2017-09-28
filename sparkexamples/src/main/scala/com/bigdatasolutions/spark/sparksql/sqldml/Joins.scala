package com.bigdatasolutions.spark.sparksql.sqldml

import com.madhukaraphatak.sparktraining.sparksql.utils.Sales
import org.apache.hadoop.hive.ql.parse.JoinType
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.Inner

/**
 * Running join queries on schemaRDD
 */
object Joins {

    def main(args: Array[String]) {
      //Register the table - sales
      val sc: SparkContext = new SparkContext(args(0), "joins")
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))
      salesDf.registerTempTable("sales")

      //Register the table customer
      val customerDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(2))
      customerDf.registerTempTable("customer")

      joinQuery(sqlContext)
      nestedQuery(sqlContext)
    }

   def joinQuery(sqlcontext:SQLContext){
    //Join two tables
    val customerJoinSales = sqlcontext.sql("select * from sales join customer  on (customer.customerId=sales.customerId)")
    customerJoinSales.show()
  }

  def nestedQuery(sqlcontext:SQLContext){
    //Nested query
    val nestedOutput = sqlcontext.sql("select customerId,total_amount from ( select customerId, sum(amountPaid) total_amount from sales group by customerId ) t2 where t2.total_amount >=600")
    nestedOutput.show()
  }
}
