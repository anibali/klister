package org.nibali.klister.rdd

import org.nibali.klister.Klister._
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.nibali.klister.Similarity
import java.util.Arrays

class KlisterStringKeyRDDFunctions[V](self: RDD[(String, V)])
    (implicit vt: ClassTag[V])
  extends Logging
  with Serializable
{
  /**
   * Uses banding to prune records. Better than naive and approx implementations
   * except perhaps when the number of output records approaches n^2
   */
  def bandingSimilarityJoin[W](other: RDD[(String, W)], shingleSize:Int, thresh:Float, nReducers:Int = 1):RDD[((String,V),(String,W))] = {
    val b = 20
    var r = 5
    val nHashes = b * r
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
    val sb = hashBands(s, r)
    val tb = hashBands(t, r)
    sb.join(tb).map(_._2).filter(p => {
      Similarity.jaccard(Similarity.hashedShingles(p._1._1, shingleSize), Similarity.hashedShingles(p._2._1, shingleSize)) > thresh
    }).distinct()
  }
  
  /**
   * Takes an RDD of signature/payload pairs and produces a larger RDD of
   * band-hash/payload pairs
   */
  private def hashBands[T](rdd:RDD[(Array[Int], T)], r:Int):RDD[(Int, T)] = {
    rdd.flatMap(p => {
      var list = List[(Int, T)]()
      var i = 0
      var hash = 0
      p._1.foreach(sigPart => {
        if(i % r == 0) {
          if(i > 0) {
            list ::= (hash, p._2)
          }
          hash = 1
          hash = 31 * hash + i
        }
        hash = 31 * hash + sigPart
        i += 1
      })
      list ::= (hash, p._2)
      list
    })
  }
  
  /**
   * Uses minhashing and approximate Jaccard similarities in a full cartesian >
   * filter style algorithm.
   */
  def approxSimilarityJoin[W](other: RDD[(String, W)], shingleSize:Int, thresh:Float, nReducers:Int = 1):RDD[((String,V),(String,W))] = {
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
