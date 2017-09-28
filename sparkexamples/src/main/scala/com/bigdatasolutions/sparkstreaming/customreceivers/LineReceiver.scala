package com.bigdatasolutions.sparkstreaming.customreceivers

import java.io.{InputStreamReader, BufferedReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 *  Custom receiver to read data as line by line
 */
class LineReceiver(host:String,port:Int) extends
Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()

  }
  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(host, port)
      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        //store will store in RDD
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }



  override def onStop(): Unit = {


  }
}
