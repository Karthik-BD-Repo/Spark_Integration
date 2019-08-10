package com.spark
import org.apache.spark.{SparkConf, SparkContext}
object SparkWordCount {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf()
    conf.setAppName("FirstApplication")
    conf.setMaster("local")
    val sc =new SparkContext(conf)
    val input = sc.textFile("file:///home/sairavi/common.txt")
    val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)
    count.saveAsTextFile("file:///home/sairavi/wordcount/")
    System.out.println("OK")
  }
}
