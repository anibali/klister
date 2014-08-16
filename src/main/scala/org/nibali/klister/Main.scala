package org.nibali.klister

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Main {
  def main(args: Array[String]) {
    print("Initializing Spark context...")
    val conf = new SparkConf().setAppName("Klister")
    val sc = new SparkContext(conf)
    //sc.addSparkListener(new BenchmarkListener(System.out))
    println(" done")
    
    val rdd = sc.textFile("README.md")
    rdd.collect.foreach(println)
    
    sc.stop()
  }
}
