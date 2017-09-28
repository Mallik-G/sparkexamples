package com.bigdatasolutions.sparkstreaming.customreceivers

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by madhu on 28/2/15.
 */
object LineReceiverExample {

  def main(args: Array[String]) {

    val ssc = new StreamingContext(args(0), "linereceiverexample", Seconds(20))
    val lines = ssc.receiverStream(new LineReceiver(args(1),args(2).toInt))
    lines.count().print()
    ssc.start()
    ssc.awaitTermination()
  }


}
