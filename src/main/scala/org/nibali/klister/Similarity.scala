package org.nibali.klister

import scala.collection.mutable

object Similarity
{
  /**
   * Calculate a minhash signature
   */
  def minhashSignature(nHashes:Int, hashFn:(Int, Int) => Int, features:Set[Int]):Array[Int] = {
    val sig = Array.fill(nHashes) {Int.MaxValue}
    features.foreach(feature => {
      Range(0, nHashes).foreach(i => {
        sig(i) = math.min(sig(i), hashFn(i, feature))
      })
    })
    return sig
  }

  /**
   * Map x to another Int using the ith hash function in this family
   */
  def universalHash(i:Int, x:Int):Int = {
    val rand = new scala.util.Random(i)
    val a = rand.nextInt(Int.MaxValue - 1) + 1
    val b = rand.nextInt(Int.MaxValue - 1) + 1
    // Note: Conversion to int is same as doing 'mod <prime>' because
    // 2^31 - 1 is prime (yay)
    return (a * x + b).toInt
  }

  /**
   * Break doc into k-shingles and return a set containing the hashcode of
   * each shingle
   */
  def hashedShingles(doc:String, k:Int = 9):Set[Int] = {
    doc.toLowerCase().sliding(k).map(_.hashCode).toSet
  }
  
  /**
   * Calculate the Jaccard similarity between two sets
   */
  def jaccard[T](a:Set[T], b:Set[T]):Float = {
    var intersection = 0f
    a.foreach(elem => {
      if(b.contains(elem)) {
        intersection += 1
      }
    })
    val union:Float = a.size + b.size - intersection
    return intersection / union
  }
}
