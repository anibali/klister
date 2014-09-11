package org.nibali.klister.rdd

import org.nibali.klister.Klister._
import org.nibali.klister.Util
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
    val (r, b) = calcBandParams(thresh, 120)
    val nHashes = b * r
    
    val s = makeSignatures(self, shingleSize, nHashes)
    val t = makeSignatures(other, shingleSize, nHashes)
    
    val smap = s.keyify(0)
    val tmap = t.keyify(1)
    val sb = hashBands(smap.map(p => (p._2._1, p._1)), r)
    val tb = hashBands(tmap.map(p => (p._2._1, p._1)), r)
    
    val idPairs = sb.join(tb).map(_._2).distinct()
    val recordPairs = idPairs.join(smap.map(p => (p._1, p._2._2))).map(_._2).join(tmap.map(p => (p._1, p._2._2))).map(_._2)
    
    recordPairs.filter(p => {
      Similarity.jaccard(Similarity.hashedShingles(p._1._1, shingleSize), Similarity.hashedShingles(p._2._1, shingleSize)) > thresh
    })
  }
  
  def bandingSimilarityJoinNew[W](other: RDD[(String, W)], shingleSize:Int, thresh:Float, nReducers:Int = 1):RDD[((String,V),(String,W))] = {
    val (r, b) = calcBandParams(thresh, 120)
    val nHashes = b * r
    
    val s = makeSignatures(self, shingleSize, nHashes)
    val t = makeSignatures(other, shingleSize, nHashes)
    
    val smap = s.keyify(0)
    val tmap = t.keyify(1)
    val sb = hashBands(smap.map(p => (p._2._1, p._1)), r)
    val tb = hashBands(tmap.map(p => (p._2._1, p._1)), r)
    
    val idPairs = sb.equijoin(tb).map(_._2).distinct()
    val recordPairs = idPairs.equijoin(smap.map(p => (p._1, p._2._2))).map(_._2).equijoin(tmap.map(p => (p._1, p._2._2))).map(_._2)
    
    recordPairs.filter(p => {
      Similarity.jaccard(Similarity.hashedShingles(p._1._1, shingleSize), Similarity.hashedShingles(p._2._1, shingleSize)) > thresh
    })
  }
  
  def bandingSimilarityJoinBad[W](other: RDD[(String, W)], shingleSize:Int, thresh:Float, nReducers:Int = 1):RDD[((String,V),(String,W))] = {
    val (r, b) = calcBandParams(thresh, 120)
    val nHashes = b * r
    
    val s = makeSignatures(self, shingleSize, nHashes)
    val t = makeSignatures(other, shingleSize, nHashes)
    
    // Old-style "move everything around" version. Requires .distinct() call at
    // very end
    val sb = hashBands(s, r)
    val tb = hashBands(t, r)
    val recordPairs = sb.join(tb).map(_._2)
    
    recordPairs.filter(p => {
      Similarity.jaccard(Similarity.hashedShingles(p._1._1, shingleSize), Similarity.hashedShingles(p._2._1, shingleSize)) > thresh
    }).distinct()
  }
  
  private def calcBandParams(thresh:Float, maxHashes:Int=120) = {
    var logTerm = 0.0
    if(thresh > 0.15)
      logTerm = math.log(1 / (thresh - 0.1))
    else
      logTerm = math.log(1 / 0.15)
    val r = math.floor(Util.lambertW(maxHashes * logTerm) / logTerm).toInt
    val b = maxHashes / r
    (r, b)
  }

  private def makeSignatures[T](rdd:RDD[(String, T)], shingleSize:Int, nHashes:Int):RDD[(Array[Int], (String, T))] = {
    rdd.map(pair => {
      val shings = Similarity.hashedShingles(pair._1, shingleSize)
      val sig = Similarity.minhashSignature(nHashes, Similarity.universalHash, shings)
      (sig, pair)
    })
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
  
  def naiveSimilarityJoin[W](other: RDD[(String, W)], shingleSize:Int, thresh:Float, nReducers:Int = 1):RDD[((String,V),(String,W))] = {
    self.cartesian(other).filter(pair => {
      Similarity.jaccard(Similarity.hashedShingles(pair._1._1, shingleSize), Similarity.hashedShingles(pair._2._1, shingleSize)) > thresh
    })
  }
}
