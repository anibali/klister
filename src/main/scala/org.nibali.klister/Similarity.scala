package org.nibali.klister

import scala.collection.mutable

object Similarity
{
  def minhashSignature(nHashes:Int, hashFn:(Int, Int) => Int, features:Set[Int]):Array[Int] = {
    val sig = Array.fill(nHashes) {Int.MaxValue}
    features.foreach(feature => {
      Range(0, nHashes).foreach(i => {
        sig(i) = math.min(sig(i), hashFn(i, feature))
      })
    })
    return sig
  }

  def universalHash(index:Int, x:Int):Int = {
    val rand = new scala.util.Random(index)
    val a = rand.nextInt(Int.MaxValue - 1) + 1
    val b = rand.nextInt(Int.MaxValue - 1) + 1
    // Note: Conversion to int is same as doing 'mod <prime>' because
    // 2^31 - 1 is prime (yay)
    return (a * x + b).toInt
  }

  def hashedShingles(doc:String, k:Int = 9):Set[Int] = {
    doc.toLowerCase.sliding(k).map(_.hashCode).toSet
  }
}
