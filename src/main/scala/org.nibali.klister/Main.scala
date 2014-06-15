package org.nibali.klister

import org.nibali.klister.Klister._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Main extends App {
  // Create Spark configuration
  val conf = new SparkConf().setAppName("Less than join")
  // Enable event log so we can use the history server
  conf.set("spark.eventLog.enabled", "true")
  conf.set("spark.eventLog.dir", "hdfs://localhost:8020/user/cloudera/spark_logs")
  // Create Spark context
  val sc = new SparkContext(conf)

  val rdds = args.take(2).map(file => {
    sc.textFile(file).map(x => x.split("\\W+").map(_.toInt)).map(a => (a(0), a(1)))
  })
  val nReducers = args(2).toInt

  var joined:RDD[((Int, Int),(Int, Int))] = null
  args(3) match {
    case "theta" =>
      joined = rdds(0).thetaJoin(rdds(1), _ < _, nReducers)
    case "cartesian" =>
      joined = rdds(0).cartesian(rdds(1)).filter(x => x._1._1 < x._2._1)
    case "inequality" =>
      joined = rdds(0).inequalityJoin(rdds(1), Comparison.Less, nReducers)
    case _ =>
      println("Valid execution types: theta, cartesian, inequality")
  }

  if(joined != null)
  {
    println("Output contains " + joined.count() + " records")

    println("Reducer output record counts:")

    joined.mapPartitions(iter => {
      var cnt = 0
      while (iter.hasNext) {
        iter.next
        cnt += 1
      }
      List(cnt).iterator
    }).collect().foreach(println)
  }

  // Stop Spark context
  sc.stop()
}
