package org.nibali.klister

import org.nibali.klister.Klister._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Main extends App {
  similarity()

  def similarity() {
    // Create Spark configuration
    val conf = new SparkConf().setAppName("Similarity")
    // Enable event log so we can use the history server
    //conf.set("spark.eventLog.enabled", "true")
    //conf.set("spark.eventLog.dir", "hdfs://localhost:8020/user/cloudera/spark_logs")
    // Create Spark context
    val sc = new SparkContext(conf)
    //sc.addSparkListener(new BenchmarkListener(System.out))

    val nReducers = args(2).toInt
    val rdds = args.slice(0, 2).map(file => {
      sc.textFile(file, math.round(math.sqrt(nReducers)).toInt).
        map(_.split("\t")).
        filter(_.size == 2).
        map(a => (a(0), a(1))).
        sample(false, 0.02, 0)
    })

    val joined = rdds(0).naiveSimilarityJoin(rdds(1), 3, 0.4f, nReducers)

    val counts = joined.mapPartitions(iter => List(iter.size).iterator).collect()
    println("Output contains " + counts.sum + " records")
    println("Individual partition record counts:")
    counts.foreach(println)
    joined.take(5).foreach(println)

    // Stop Spark context
    sc.stop()
  }

  def lessThan() {
    // Create Spark configuration
    val conf = new SparkConf().setAppName("Less than")
    // Enable event log so we can use the history server
    //conf.set("spark.eventLog.enabled", "true")
    //conf.set("spark.eventLog.dir", "hdfs://localhost:8020/user/spark/applicationHistory")
    // Create Spark context
    val sc = new SparkContext(conf)
    //sc.addSparkListener(new BenchmarkListener(System.out))

    val nReducers = args(2).toInt
    val rdds = args.slice(0, 2).map(file => {
      sc.textFile(file, math.round(math.sqrt(nReducers)).toInt).
        map(x => x.split("\\W+").map(_.toInt)).map(a => (a(0), a(1)))
    })

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
      val counts = joined.mapPartitions(iter => List(iter.size).iterator).collect()
      println("Output contains " + counts.sum + " records")
      println("Individual partition record counts:")
      counts.foreach(println)
    }

    // Stop Spark context
    sc.stop()
  }
}
