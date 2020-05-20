package com.adrien.spark

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
 * Created by wuyufei on 06/09/2017.
 */

class CustomReceiver (host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
  override def onStart(): Unit = {
    // 自定义一个线程实现 receiver 方法
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  override def onStop(): Unit = {
  }

  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      //新建一个 socket 连接
      socket = new Socket(host, port)
      //获取 Socket 连接
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      userInput = reader.readLine()
      //如果没有停止 且 lines 不是零
      while(!isStopped && userInput != null) {
        //通过store将获取到的lines提交给 spark 集群
        store(userInput)
        //下一行
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>


      case t: Throwable =>

        restart("Error receiving data", t)
    }
  }
}

object CustomReceiver {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.receiverStream(new CustomReceiver("hadoop102", 9999))
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}