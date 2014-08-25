package org.nibali.klister.rdd

import org.nibali.klister.Klister._
import org.nibali.klister.Util
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.nibali.klister.regionmaps.RegionMapper
import org.nibali.klister.regionmaps.SimpleRegionMapper

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
  
  /**
   * Creates a unique ID for each record in the RDD
   * Using a different "brand" for different RDDs will result in the keys
   * being unique across those RDDs
   */
  def keyify(brand:Int=0) = {
    val partitionIndexBits = Util.log2(self.partitions.length)
    self.mapPartitionsWithIndex((index:Int, iter: Iterator[T]) => {
      var i = 0L
      iter.map(elem => {
        i += 1
        (((i << partitionIndexBits) + index) | (brand << 60), elem)
      })
    })
  }
}
