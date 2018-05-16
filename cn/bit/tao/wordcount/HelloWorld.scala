package cn.bit.tao.wordcount

import org.apache.spark._

/**
  * 功能：WordCount
  */
object WordCount{
  def main (args: Array[String] ): Unit = {
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val file=sc.textFile("D:\\spark_data\\readme.txt")
    val counts=file.flatMap(line=>line.split(" "))
      .map(word=>(word,1))
      .reduceByKey(_+_)
    counts.saveAsTextFile("D:\\spark_data\\result.txt")
  }
}
