package org.nibali.klister

import org.nibali.klister.Klister._

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.SparkContext._

/**
* Extra functions on pair RDDs provided by Klister through an implicit
* conversion. Import `org.nibali.klister.Klister._` at the top of your program
* to use these functions.
*/
class KlisterOrderedRDDFunctions[T:Ordering:ClassTag](self: RDD[T])
  extends Logging
  with Serializable
{
  private val ordering = implicitly[Ordering[T]]

  /**
  * Sample with a fraction of the RDD, then find all q-quantiles. Note that
  * the records sampled from the RDD MUST all fit in one reducer
  */
  private[klister] def quantiles(q:Int, fraction:Float=1):Seq[T] = {
    val sampled = self.sample(false, fraction)
    val step = sampled.count().toFloat / q
    val quants = sampled.coalesce(1, true).glom().flatMap(a => {
      util.Sorting.quickSort(a)
      (1 until q).map(i => {
        a(math.ceil(i * step).toInt - 1)
      })
    })
    return quants.collect()
  }

  private def findBucket[W:Ordering](boundaries:Seq[W], elem:W):Int = {
    // TODO: optimise with binary search
    val ord = Ordering[W]
    var bucket = 0
    boundaries.foreach(b =>
      if(ord.gt(elem, b))
        bucket += 1
    )
    return bucket
  }

  private[klister] def histo(boundaries:Seq[T]):Array[Int] = {
    val sums = self.map(x =>
      (findBucket(boundaries, x), 1)
    ).reduceByKey((x:Int, y:Int) => x + y).collect()
    val hg = new Array[Int](boundaries.size + 1)
    sums.foreach(pair => hg(pair._1) = pair._2)
    return hg
  }
}
