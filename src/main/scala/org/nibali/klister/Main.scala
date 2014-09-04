package org.nibali.klister

import org.nibali.klister.Klister._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]) {

    RunConfig.parse(args).map(config => {
      print("[KLISTER] Initializing Spark context...")
      val sparkConf = new SparkConf().setAppName("Klister")
      val sc = new SparkContext(sparkConf)
      //sc.addSparkListener(new BenchmarkListener(System.out))
      println(" done")
      
      var nRecords = -1L
      var nMatches = -1L

      val elapsed = Util.time({
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", config.awsAccessKeyId)
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", config.awsSecretAccessKey)
        
        val tweets = sc.textFile(config.inputPath)
        val sampledTweets = tweets.sample(false, config.records / tweets.count.toFloat, 1234).repartition(config.nodes)
        val numberedTweets = sampledTweets.keyify().map(_.swap)
        nRecords = numberedTweets.count()

        var joined:RDD[((String, Long), (String, Long))] = null
        config.joinType match {
          case "similarity-approx" =>
            println("[KLISTER] Approximate similarity join")
          	// 4000 tweets, 18 matches, 16.66 seconds
          	joined = numberedTweets.approxSimilarityJoin(numberedTweets, 5, config.threshold, config.nodes)
          case "similarity-banding" =>
            println("[KLISTER] Banding similarity join")
            // 4000 tweets, 19 matches, 6.22 seconds
            joined = numberedTweets.bandingSimilarityJoin(numberedTweets, 5, config.threshold, config.nodes)
        }

        val different = joined.filter(a => a._1._2 > a._2._2).filter(a => !a._1._1.equals(a._2._1))
        
        nMatches = different.count()
      })
      
      printf("[KLISTER] Number of records: %d\n", nRecords)
      printf("[KLISTER] Total matches: %d\n", nMatches)
      printf("[KLISTER] Elapsed time: %.2f s\n", elapsed / 1000f)

      sc.stop()
    })
  }
}
