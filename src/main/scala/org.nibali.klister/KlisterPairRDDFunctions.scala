package org.nibali.klister

import org.nibali.klister.okcan.MatrixPartitioner
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
class KlisterPairRDDFunctions[K, V](self: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging
  with Serializable
{
  private val ordering = implicitly[Ordering[K]]

  def thetaJoin[W](other: RDD[(K, W)], joinCond:((K, K) => Boolean), nReducers:Int = 1):RDD[((K, V),(K, W))] = {
    return self.kartesian(other, nReducers).
      filter(x => joinCond.apply(x._1._1, x._2._1))
  }

  def inequalityJoin[W](other: RDD[(K, W)], op: Comparison.Comparison, nReducers:Int = 1):RDD[((K, V),(K, W))] = {
    if(ordering == null) {
      throw new RuntimeException("Key type does not have an Ordering")
    }
    // TODO: Preprocessing to optimise join
    return self.thetaJoin(other, (a, b) => op.get.contains(ordering.compare(a, b)), nReducers)
  }

  def equijoin[W](other: RDD[(K, W)], nReducers:Int = 1):RDD[(K,(V, W))] = {
    return self.inequalityJoin(other, Comparison.Eq, nReducers).
      map(x => (x._1._1, (x._1._2, x._2._2)))
  }
}
