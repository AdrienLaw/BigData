package com.adrien.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount extends App {

  //需要新建一个 SparkConf 变量来提供配置
  val sparkConf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[4]")
  //需要一个StreamingContext入口
  val ssc = new StreamingContext(sparkConf,Seconds(1))

  //从hadoop102:9999 不断获取数据
  val lines = ssc.socketTextStream("hadoop102",9999)
  //将每一行通过空格分割而成多个单词
  val words = lines.flatMap(_.split(" "))
  //将每一个单词转换为一个元组
  val pairs = words.map((_,1))
  //根据单词 统计数量
  val result = pairs.reduceByKey(_+_)
  //结果打印
  result.print()
  ssc.start()
  ssc.awaitTermination()
}
