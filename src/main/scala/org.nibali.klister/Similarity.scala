package org.nibali.klister

import scala.collection.mutable

object Similarity
{
  def test(shingleSize:Int = 5) {
    val matrix = new CharacteristicMatrix()
    val docs = List(
      "Hello there Rob",
      "Hello there Bob",
      "I want to rob a bank",
      "Cheese",
      "Hello there Rob"
    )
    var i = 0
    docs.foreach(doc => {
      matrix.add(hashedShingles(doc, shingleSize), i)
      i += 1
    })
    // Compute signature matrix
    val perms = 1009 // Current hash functions require this to be prime
    val arr = Array.fill(perms, docs.size) {Int.MaxValue}

    val rowLabels = matrix.rowLabels.toArray
    Range(0, rowLabels.size).foreach(i => {
      matrix.getRow(rowLabels(i)).foreach(col => {
        Range(0, perms).foreach(seed => {
          if(hash(seed, i, perms) < arr(seed)(col))
            arr(seed)(col) = hash(seed, i, perms)
        })
      })
    })
    
    // Calculate approximate Jaccard similarities
    for(x <- Range(0,docs.size); y <- Range(0,docs.size)) {
      if(x < y) {
        println(docs(x), docs(y))
        var matches = 0f
        Range(0, perms).foreach(i => {
          if(arr(i)(x) == arr(i)(y)) {
            matches += 1
          }
        })
        // Approximate similarity
        println("%.2f%%".format(100 * matches / perms))
      }
    }
  }

  def hash(index:Int, x:Int, mod:Int):Int = {
    if(index == 0) {
      return x
    } else {
      val rand = new scala.util.Random(index)
      return ((x * (index * 2) + rand.nextInt) % mod + mod) % mod
    }
  }

  def hashedShingles(doc:String, k:Int = 9):Set[Int] = {
    doc.toLowerCase.sliding(k).map(_.hashCode).toSet
  }

  class CharacteristicMatrix {
    private val map = mutable.Map[Int, mutable.Set[Int]]()
    private val cols = mutable.Set[Int]()

    def add(rows:Set[Int], col:Int) {
      rows.foreach(row => {
        if(map.contains(row))
          map(row).add(col)
        else
          map(row) = mutable.Set[Int](col)
      })
      cols.add(col)
    }

    def rowLabels:Iterable[Int] = {
      return map.keys
    }

    def colLabels:Iterable[Int] = {
      return cols
    }

    def getRow(row:Int):mutable.Set[Int] = {
      return map(row)
    }

    def contains(row:Int, col:Int):Boolean = {
      return map(row).contains(col)
    }
  }
}
