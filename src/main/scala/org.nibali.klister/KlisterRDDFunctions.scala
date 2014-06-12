package org.nibali.klister

import org.nibali.klister.Klister._

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.SparkContext._

/**
* Extra functions on RDDs provided by Klister through an implicit
* conversion. Import `org.nibali.klister.Klister._` at the top of your program
* to use these functions.
*/
class KlisterRDDFunctions[T:ClassTag](self: RDD[T])
  extends Logging
  with Serializable
{
  def kartesian[U:ClassTag](other: RDD[U], nReducers:Int=1):RDD[(T,U)] = {
    val srm = SimpleRegionMapper(self, other, nReducers)
    return _kartesian(other, srm)
  }

  private[klister] def _kartesian[U:ClassTag](other: RDD[U], rm:RegionMapper[T,U]):RDD[(T,U)] = {
    val ss = self.flatMap(e => {rm.getSRegions(e).map(region => (region, e))})
    val tt = other.flatMap(e => {rm.getTRegions(e).map(region => (region, e))})

    return ss.join(tt, rm.nReducers).map(_._2)
  }
}
