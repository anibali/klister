package org.nibali.klister

import org.nibali.klister.Klister._

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.SparkContext._

class KlisterStringKeyRDDFunctions[V](self: RDD[(String, V)])
    (implicit vt: ClassTag[V])
  extends Logging
  with Serializable
{
  def naiveSimilarityJoin[W](other: RDD[(String, W)], shingleSize:Int, thresh:Float, nReducers:Int = 1):RDD[((String,V),(String,W))] = {
    val nHashes = 100
    val s = self.map(pair => {
      val shings = Similarity.hashedShingles(pair._1, shingleSize)
      val sig = Similarity.minhashSignature(nHashes, Similarity.universalHash, shings)
      (sig, pair)
    })
    val t = other.map(pair => {
      val shings = Similarity.hashedShingles(pair._1, shingleSize)
      val sig = Similarity.minhashSignature(nHashes, Similarity.universalHash, shings)
      (sig, pair)
    })
    return s.thetaJoin(t, (a, b) => {
      var matches = 0f
      Range(0, nHashes).foreach(i => {
        if(a(i) == b(i)) {
          matches += 1
        }
      })
      matches / nHashes > thresh
    }, nReducers).map(x => (x._1._2, x._2._2))
  }
}
