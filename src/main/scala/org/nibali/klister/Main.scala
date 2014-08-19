package org.nibali.klister

import org.nibali.klister.Klister._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]) {
    print("Initializing Spark context...")
    val conf = new SparkConf().setAppName("Klister")
    val sc = new SparkContext(conf)
    //sc.addSparkListener(new BenchmarkListener(System.out))
    println(" done")
    
    val elapsed = Util.time({
      val tweets = sc.textFile("data/tweets.txt", 1)
      val nums = sc.parallelize(1 to tweets.count().toInt, 1)
      val numberedTweets = tweets.zip(nums)
      
      // 4000 tweets, 18 matches, 17.35 seconds
      //val joined = numberedTweets.approxSimilarityJoin(numberedTweets, 5, 0.7f, 1)
      // 4000 tweets, 19 matches, 14.37 seconds
      val joined = numberedTweets.bandingSimilarityJoin(numberedTweets, 5, 0.7f, 1)
      
      val different = joined.filter(a => a._1._2 > a._2._2).filter(a => !a._1._1.equals(a._2._1))
      
      //different.foreach(println)
      printf("Total matches: %d\n", different.count())
    })
    printf("Elapsed time: %.2f s\n", elapsed / 1000f)
    
    sc.stop()
  }
}
