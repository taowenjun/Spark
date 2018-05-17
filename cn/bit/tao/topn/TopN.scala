package cn.bit.tao.topn

import org.apache.spark._

import scala.collection.SortedMap

object TopN{

  def main(args:Array[String]): Unit ={
     val sparkConf = new SparkConf().setAppName("TopN").setMaster("local")
     val sc = new SparkContext(sparkConf)

    val N = sc.broadcast(5)
    val path = "D:\\upload\\topn"
    val outPath="D:\\upload\\result"

    val input = sc.textFile(path)
    val pair = input.map(line=>{
      val tokens = line.split("\t")
      (tokens(1).toInt,tokens)
    })

    import Ordering.Implicits._
    val partitions = pair.mapPartitions(itr => {
        var sortedMap = SortedMap.empty[Int,Array[String]]
        itr.foreach { tuple =>
        {
             sortedMap += tuple
             if(sortedMap.size > N.value){
               sortedMap = sortedMap.takeRight(N.value)
             }
        }
        }
       sortedMap.takeRight(N.value).toIterator
    })

    val alltop10 = partitions.collect()
    val finaltop10 = SortedMap.empty[Int,Array[String]].++:(alltop10)
    val resultUsingMapPartition = finaltop10.takeRight(N.value)

    resultUsingMapPartition.foreach{
      case (k,v) => println(s"$k\t${v.asInstanceOf[Array[String]].mkString(",")}")
    }

    val moreConciseApproach = pair.groupByKey().sortByKey(false).take(N.value)

    moreConciseApproach.foreach{
      case (k,v) =>println(s"$k\t${v.flatten.mkString(",")}")
    }

    sc.stop()
  }

}