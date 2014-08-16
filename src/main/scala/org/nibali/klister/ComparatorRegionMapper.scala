package org.nibali.klister.regionmaps

import org.nibali.klister.Klister._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object ComparatorRegionMapper
{
  def apply[K:ClassTag:Ordering,V,W](s:RDD[(K,V)], t:RDD[(K,W)], op:Comparison.Comparison, nReducers:Int): RegionMapper[(K,V),(K,W)] = {
    val sCount = s.count()
    val tCount = t.count()
    val sKeys = s.map(_._1)
    val tKeys = t.map(_._1)
    val bothKeys = sKeys.union(tKeys)

    // These calculations could be tuned
    var q = nReducers * 4
    val nSamples = math.min(q * 32, sCount + tCount).toInt
    if(nSamples < q) {
      q = nSamples - 1
    }

    var candidateRanges = List[Range2D[K]]()
    if(q > 0) {
      val quants = bothKeys.quantiles(q, nSamples.toFloat/(sCount + tCount)).distinct.toArray
      val sHistogram = sKeys.histo(quants)
      val tHistogram = tKeys.histo(quants)
      for(i <- 0 until sHistogram.size) {
        for(j <- 0 until tHistogram.size) {
          if((i == j || op.get.contains(i.compareTo(j))) && sHistogram(i) > 0 && tHistogram(j) > 0) {
            candidateRanges = new Range2D(
              quants.lift(i-1), quants.lift(j-1), quants.lift(i), quants.lift(j)
            ) :: candidateRanges
          }
        }
      }
    }

    return new PrunedRegionMapper[K,V,W](candidateRanges, nReducers)
  }
}
