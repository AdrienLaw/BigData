package com.adrien.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object QueueRdd {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("QueueRdd")
    val ssc = new StreamingContext(conf,Seconds(5))
    //创建RDD队列
    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]] ()
    // 创建QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue)
    //处理队列中的RDD数据
    val mappedStream = inputStream.map(x => (x % 10,1))
    val reducedStream = mappedStream.reduceByKey(_+_)
    //打印结果
    reducedStream.print()
    //启动计算
    ssc.start()

    for (i <- 1 to 30) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(3000)
      //通过程序停止StreamingContext的运行
      //ssc.stop()
    }
  }
}
