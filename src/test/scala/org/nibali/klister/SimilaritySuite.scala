package org.nibali.klister

import org.scalatest.FunSuite
import org.scalautils.Tolerance._
 
class SimilaritySuite extends FunSuite {
  test("minhashSignature") {
    val shingleSize = 3
    val nHashes = 100

    val docs = List(
      "Hello there Rob",
      "Hello there Bob",
      "I want to rob a bank",
      "Cheese",
      "Hello there Rob"
    )
    val shingled = docs.map(Similarity.hashedShingles(_, shingleSize))
    val sigs = shingled.map(Similarity.minhashSignature(nHashes, Similarity.universalHash, _))

    for(x <- Range(0,docs.size); y <- Range(0,docs.size)) {
      if(x < y) {
        var matches = 0f
        Range(0, nHashes).foreach(i => {
          if(sigs(x)(i) == sigs(y)(i)) {
            matches += 1
          }
        })
        // Estimated Jaccard similarity
        val approx = matches / nHashes
        // Actual Jaccard similarity
        val exact = shingled(x).intersect(shingled(y)).size / shingled(x).union(shingled(y)).size.toFloat
        assert(approx === (exact +- 0.05f))
      }
    }
  }
}