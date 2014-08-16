package org.nibali.klister.rdd

import org.nibali.klister.Klister._
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.nibali.klister.Util

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
    val samples = self.sample(false, fraction).collect().sorted.toArray
    val step = samples.size.toFloat / q
    val quants = (1 until q).map(i => {
      samples(math.ceil(i * step).toInt - 1)
    })
    return quants
  }

  /**
  * Generate a histogram for the RDD with buckets defined by the specified
  * boundaries
  */
  private[klister] def histo(boundaries:Seq[T]):Array[Int] = {
    val sums = self.map(x =>
      (Util.findBucket(boundaries, x), 1)
    ).reduceByKey((x:Int, y:Int) => x + y).collect()
    val hg = new Array[Int](boundaries.size + 1)
    sums.foreach(pair => hg(pair._1) = pair._2)
    return hg
  }
}
