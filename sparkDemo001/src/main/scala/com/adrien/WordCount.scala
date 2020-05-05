package com.adrien

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App{
  val sparkConf = new SparkConf().setAppName("WordCount").setIfMissing("spark.ui.port","hadoop102").set("spark.ui.port","7077")
  .setMaster("spark://hadoop102:7077")
  .set("executor-memory","500M")
  .set("executor-limit","1")
  .set("driver-memory","500M");
  val sparkContext = new SparkContext(sparkConf)
  val file = sparkContext.textFile("hdfs://hadoop102:9000/wc.input")
  val words = file.flatMap(_.split(" "))
  val wordsTuple = words.map((_,1))
  wordsTuple.reduceByKey(_+_).saveAsTextFile("hdfs://hadoop102:9000/out3")
  sparkContext.stop()
}/*
[root@hadoop102 spark-2.1.1-bin-hadoop2.7]# bin/spark-submit --class com.adrien.WordCount --master spark://hadoop102:7077 --executor-memory 512M --total-executor-cores 3 /opt/module/spark-2.1.1-bin-hadoop2.7/sparkDemo001-1.0-SNAPSHOT.jar hdfs://hadoop102:9000/wc.input hdfs://hadoop102:9000/out3*/
/*
[root@hadoop102 hadoop-2.7.2]# bin/hdfs dfs -cat /out3/p*
(Celine,1)
(drien,1)
(adrien,1)
(daniel,1)
(jennifer,1)
(kim,2)*/
