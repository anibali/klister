package org.nibali.klister

import org.nibali.klister.okcan.MatrixPartitioner
import org.nibali.klister.Klister._

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.SparkContext._

private object Cached {
  val counts = collection.concurrent.TrieMap[Object, Long]()
}

/**
* Extra functions on RDDs provided by Klister through an implicit
* conversion. Import `org.nibali.klister.Klister._` at the top of your program
* to use these functions.
*/
class KlisterRDDFunctions[T:ClassTag](self: RDD[T])
  extends Logging
  with Serializable
{
  /**
  * Like count(), but caches the result.
  */
  def kount[U]():Long = {
    if(!Cached.counts.contains(self)) {
      Cached.counts(self) = self.count()
    }
    return Cached.counts(self)
  }

  def kartesian[U:ClassTag](other: RDD[U], nReducers:Int = 1):RDD[(T,U)] = {
    var s = self
    var t = other
    val sCount = s.kount().toInt
    val tCount = t.kount().toInt

    val mp = new MatrixPartitioner(sCount, tCount, nReducers)

    val ss = s.flatMap(e => {
      val row = math.abs(util.Random.nextLong()) % sCount
      mp.regionsForRow(row).map(region => (region, e))
    })

    val tt = t.flatMap(e => {
      val col = math.abs(util.Random.nextLong()) % tCount
      mp.regionsForCol(col).map(region => (region, e))
    })

    val cross = ss.join(tt, nReducers).map(_._2)

    return cross
  }
}
